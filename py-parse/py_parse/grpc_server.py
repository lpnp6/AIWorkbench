"""gRPC server exposing `parse.v1.ParseService` (async task model).

The server is a thin control plane:

    SubmitParse  →  validate scheme, reject a known source, append a PENDING row
    GetResult    →  poll the TaskStore (Postgres status, Redis chunks cache)
    RequeueParse →  insert a new PENDING row for a FAILED task's document
    ListTasks    → report the newest row of each source (optionally filtered)

The heavy read→extract→chunk work runs in a separate Celery worker; a separate
dispatcher publishes outbox rows to the queue, so this process never talks to
the broker.
"""
from __future__ import annotations

import logging
import uuid
from concurrent import futures

import grpc

from .adapters.source_reader import SourceReaderRegistry
from .config import Settings
from .domain.models import Chunk
from .factory import build_registry, build_store
from .generated import (
    ChunkMetadata,
    DocMeta,
    GetResultRequest,
    GetResultResponse,
    ParseChunk,
    ListTasksRequest,
    ListTasksResponse,
    ParseRequest,
    ParseResult,
    ParseServiceServicer,
    RequeueParseRequest,
    RequeueParseResponse,
    SubmitParseResponse,
    TaskStatus as PbTaskStatus,
    TaskSummary as PbTaskSummary,
    add_ParseServiceServicer_to_server,
)
from .ports.source_reader import UnsupportedSchemeError
from .ports.task_store import (
    TaskSnapshot,
    TaskStatus as StoreStatus,
    TaskStore,
    TaskSummary as TaskSummaryRecord,
)

logger = logging.getLogger("py_parse.grpc")


_STATUS_TO_PB = {
    StoreStatus.PENDING: PbTaskStatus.TASK_STATUS_PENDING,
    StoreStatus.RUNNING: PbTaskStatus.TASK_STATUS_RUNNING,
    StoreStatus.SUCCESS: PbTaskStatus.TASK_STATUS_SUCCESS,
    StoreStatus.FAILED: PbTaskStatus.TASK_STATUS_FAILED,
}


def _to_pb_task_summary(task: TaskSummaryRecord) -> PbTaskSummary:
    pb = PbTaskSummary(
        task_id=task.task_id,
        doc_id=task.doc_id,
        source=task.source,
        status=_STATUS_TO_PB[task.status],
        error=task.error,
    )
    # The columns are timestamp-without-time-zone holding UTC, which is exactly
    # how Timestamp.FromDatetime interprets a naive datetime.
    if task.created_at is not None:
        pb.created_at.FromDatetime(task.created_at)
    if task.updated_at is not None:
        pb.updated_at.FromDatetime(task.updated_at)
    return pb


def _to_pb_chunk(chunk: Chunk) -> ParseChunk:
    return ParseChunk(
        chunk_id=chunk.chunk_id,
        text=chunk.text,
        metadata=ChunkMetadata(
            page=chunk.metadata.page,
            heading=chunk.metadata.heading,
            chunk_type=chunk.metadata.chunk_type,
        ),
    )


class ParserServicer(ParseServiceServicer):
    """Async submit/poll implementation of `ParseService`."""

    def __init__(self, registry: SourceReaderRegistry, store: TaskStore) -> None:
        self._registry = registry
        self._store = store

    def SubmitParse(self, request: ParseRequest, context: grpc.ServicerContext) -> SubmitParseResponse:
        doc_id = request.doc_id
        source = request.source
        logger.info("SubmitParse: doc_id=%r source=%r", doc_id, source)

        # Cheap scheme validation up front (the actual read happens in the worker).
        try:
            self._registry.resolve(source)
        except UnsupportedSchemeError as e:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
            return

        task_id = uuid.uuid4().hex
        try:
            # The store owns the one-task-per-source rule: the check and the
            # insert happen in one transaction, so concurrent submits of the
            # same source cannot both win.
            created = self._store.create_task(task_id, doc_id, source)
        except Exception as e:  # noqa: BLE001 - storage failure is a server error
            logger.exception("failed to record task for doc_id=%r", doc_id)
            context.abort(grpc.StatusCode.INTERNAL, f"failed to record task: {e}")
            return

        if not created.created:
            logger.info(
                "SubmitParse rejected duplicate source %r (existing task %s)",
                source,
                created.task_id,
            )
            context.abort(
                grpc.StatusCode.ALREADY_EXISTS,
                f"source already has a parse task ({created.task_id}); "
                "retry a FAILED task with RequeueParse",
            )
            return

        logger.info("SubmitParse recorded in outbox: task_id=%r", task_id)
        return SubmitParseResponse(task_id=task_id)

    def GetResult(self, request: GetResultRequest, context: grpc.ServicerContext) -> GetResultResponse:
        task_id = request.task_id
        snapshot = self._store.get_snapshot(task_id)
        if snapshot is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"unknown task_id: {task_id!r}")
            return

        return _snapshot_to_response(snapshot)

    def RequeueParse(
        self, request: RequeueParseRequest, context: grpc.ServicerContext
    ) -> RequeueParseResponse:
        task_id = request.task_id
        logger.info("RequeueParse: task_id=%r", task_id)
        try:
            result = self._store.requeue_failed(task_id)
        except Exception as e:  # noqa: BLE001 - storage failure is a server error
            logger.exception("failed to requeue task=%r", task_id)
            context.abort(grpc.StatusCode.INTERNAL, f"failed to requeue task: {e}")
            return

        if not result.found:
            context.abort(grpc.StatusCode.NOT_FOUND, f"unknown task_id: {task_id!r}")
            return
        if result.new_task_id is None:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"task {task_id!r} is {result.status.value if result.status else '?'}, "
                "only FAILED tasks can be requeued",
            )
            return

        logger.info(
            "RequeueParse created task %r for failed task %r",
            result.new_task_id,
            task_id,
        )
        return RequeueParseResponse(
            task_id=result.new_task_id, requeued_from=task_id
        )

    def ListTasks(
        self, request: ListTasksRequest, context: grpc.ServicerContext
    ) -> ListTasksResponse:
        sources = list(request.sources)
        try:
            tasks = self._store.list_tasks(sources)
        except Exception as e:  # noqa: BLE001 - storage failure is a server error
            logger.exception("failed to list tasks")
            context.abort(grpc.StatusCode.INTERNAL, f"failed to list tasks: {e}")
            return

        logger.info(
            "ListTasks: %d task(s) for %d source(s)",
            len(tasks),
            len(sources),
        )
        return ListTasksResponse(
            tasks=[_to_pb_task_summary(t) for t in tasks]
        )


def _snapshot_to_response(snapshot: TaskSnapshot) -> GetResultResponse:
    resp = GetResultResponse(
        task_id=snapshot.task_id,
        status=_STATUS_TO_PB[snapshot.status],
    )
    if snapshot.status == StoreStatus.FAILED:
        resp.error = snapshot.error
    elif snapshot.status == StoreStatus.SUCCESS:
        resp.chunks.extend(_to_pb_chunk(c) for c in snapshot.chunks)
        if snapshot.doc_meta is not None:
            resp.result.CopyFrom(
                ParseResult(
                    chunk_count=len(snapshot.chunks),
                    doc_meta=DocMeta(
                        total_pages=snapshot.doc_meta.total_pages,
                        title=snapshot.doc_meta.title,
                        warning=snapshot.doc_meta.warning,
                    ),
                )
            )
    return resp


def build_server(settings: Settings) -> grpc.Server:
    """Create a configured-but-not-started gRPC server (health + ParseService)."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=settings.grpc_max_workers))

    registry = build_registry(settings)
    store = build_store(settings)

    add_ParseServiceServicer_to_server(ParserServicer(registry, store), server)

    # Standard gRPC health service (grpc-health-probe compatible).
    try:
        from grpc_health.v1 import health, health_pb2, health_pb2_grpc

        health_servicer = health.HealthServicer()
        health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
        # "" sets the overall status (what an unnamed HealthCheckRequest probes);
        # the named entry lets callers pin the check to ParseService specifically.
        health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
        health_servicer.set("parse.v1.ParseService", health_pb2.HealthCheckResponse.SERVING)
    except ImportError:
        logger.warning("grpc_health not available; health service not registered")

    return server


def serve(settings: Settings) -> None:
    """Run the gRPC server, blocking until SIGTERM."""
    server = build_server(settings)
    address = f"{settings.grpc_host}:{settings.grpc_port}"
    server.add_insecure_port(address)
    server.start()
    logger.info("ParseService listening on %s (mineru=%s)", address, settings.mineru_url)
    server.wait_for_termination()
