"""PostgreSQL outbox store + Redis chunks cache.

PostgreSQL owns task status, doc metadata and chunks. Redis caches only the
chunks of successful tasks — status polls always hit the database, so losing or
evicting the cache costs one extra query, never correctness.

Write ordering for a result is deliberate:

    Postgres commit (task SUCCESS + chunks)  →  Redis cache  →  Celery SUCCESS

so a cached result can never exist without the durable row behind it.
"""
from __future__ import annotations

import json
import logging
import uuid

import redis as redis_lib
from sqlalchemy import func, or_, select, update

from ...domain.models import Chunk, ChunkMetadata, DocMeta
from ...ports.task_store import (
    CreateTaskResult,
    DispatchRecord,
    RequeueResult,
    TaskSnapshot,
    TaskStatus,
    TaskStore,
    TaskSummary,
)
from .models import ChunkRow, OutboxTaskRow

_LOG = logging.getLogger("py_parse.task_store")
_CHUNKS_KEY = "pyparse:chunks:{task_id}"


class RedisPostgresTaskStore(TaskStore):
    """Postgres-backed outbox task store with a Redis cache for chunks."""

    def __init__(self, redis_client: redis_lib.Redis, session_factory, ttl_seconds: int) -> None:
        self._redis = redis_client
        self._sessions = session_factory
        self._ttl = ttl_seconds

    # ---- outbox ----

    def create_task(self, task_id: str, doc_id: str, source: str) -> CreateTaskResult:
        with self._sessions() as s:
            # A transaction-scoped advisory lock on the source serializes the
            # check and the insert. SELECT ... FOR UPDATE cannot do this job:
            # when no row exists yet there is nothing to lock, and two
            # concurrent submitters would both pass the check. The lock is
            # released when this transaction ends; a hash collision (two
            # different sources sharing a lock) only costs serialization.
            s.execute(
                select(func.pg_advisory_xact_lock(func.hashtextextended(source, 0)))
            )
            existing = s.execute(
                select(OutboxTaskRow.task_id)
                .where(OutboxTaskRow.source == source)
                .limit(1)
            ).scalar_one_or_none()
            if existing is not None:
                return CreateTaskResult(created=False, task_id=existing)
            s.add(
                OutboxTaskRow(
                    task_id=task_id,
                    doc_id=doc_id,
                    source=source,
                    status=TaskStatus.PENDING.value,
                    error=None,
                )
            )
            s.commit()
        return CreateTaskResult(created=True, task_id=task_id)

    def recreate_task(
        self, task_id: str, doc_id: str, source: str, requeued_from: str
    ) -> None:
        # No source check by design: this is the retry path, where a second
        # task for the same source is exactly what is wanted.
        with self._sessions() as s:
            s.add(
                OutboxTaskRow(
                    task_id=task_id,
                    doc_id=doc_id,
                    source=source,
                    status=TaskStatus.PENDING.value,
                    error=None,
                    requeued_from=requeued_from,
                )
            )
            s.commit()

    def fetch_undispatched(
        self, limit: int, stale_seconds: int
    ) -> list[DispatchRecord]:
        # Single statement: the subquery locks the selected rows, the UPDATE
        # stamps them. A second dispatcher skips locked rows and the worker's
        # claim makes a stale re-publish harmless.
        stale_before = func.now() - func.make_interval(0, 0, 0, 0, 0, 0, stale_seconds)
        selected = (
            select(OutboxTaskRow.task_id)
            .where(
                OutboxTaskRow.status == TaskStatus.PENDING.value,
                or_(
                    OutboxTaskRow.dispatched_at.is_(None),
                    OutboxTaskRow.dispatched_at < stale_before,
                ),
            )
            .order_by(OutboxTaskRow.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
            .scalar_subquery()
        )
        with self._sessions() as s:
            rows = s.execute(
                update(OutboxTaskRow)
                .where(OutboxTaskRow.task_id.in_(selected))
                .values(dispatched_at=func.now())
                .returning(
                    OutboxTaskRow.task_id, OutboxTaskRow.doc_id, OutboxTaskRow.source
                )
            ).all()
            s.commit()
        return [
            DispatchRecord(task_id=row.task_id, doc_id=row.doc_id, source=row.source)
            for row in rows
        ]

    def release_dispatch(self, task_id: str) -> None:
        with self._sessions() as s:
            s.execute(
                update(OutboxTaskRow)
                .where(
                    OutboxTaskRow.task_id == task_id,
                    OutboxTaskRow.status == TaskStatus.PENDING.value,
                )
                .values(dispatched_at=None)
            )
            s.commit()

    # ---- worker-side transitions ----

    def try_claim(self, task_id: str) -> bool:
        # Single guarded UPDATE: PostgreSQL row locking serializes concurrent
        # queue messages, so exactly one caller observes rowcount == 1. claimed_at
        # starts the lease that fail_stale_running() measures.
        with self._sessions() as s:
            result = s.execute(
                update(OutboxTaskRow)
                .where(
                    OutboxTaskRow.task_id == task_id,
                    OutboxTaskRow.status == TaskStatus.PENDING.value,
                )
                .values(
                    status=TaskStatus.RUNNING.value,
                    claimed_at=func.now(),
                    updated_at=func.now(),
                )
            )
            claimed = result.rowcount == 1
            s.commit()
        return claimed

    def fail_stale_running(self, timeout_seconds: int) -> int:
        # One statement, so the reaper is safe with several dispatchers: the
        # status guard means only the first one to see an expired lease wins.
        # A NULL claimed_at is a row claimed before this column existed, i.e.
        # certainly older than any lease.
        claimed_before = func.now() - func.make_interval(
            0, 0, 0, 0, 0, 0, timeout_seconds
        )
        with self._sessions() as s:
            result = s.execute(
                update(OutboxTaskRow)
                .where(
                    OutboxTaskRow.status == TaskStatus.RUNNING.value,
                    or_(
                        OutboxTaskRow.claimed_at.is_(None),
                        OutboxTaskRow.claimed_at < claimed_before,
                    ),
                )
                .values(
                    status=TaskStatus.FAILED.value,
                    error=(
                        f"abandoned: no terminal write within {timeout_seconds}s "
                        "of the claim"
                    ),
                    updated_at=func.now(),
                )
            )
            s.commit()
        return result.rowcount

    def list_tasks(self, sources: list[str] | None = None) -> list[TaskSummary]:
        # DISTINCT ON keeps the newest row per source, whatever its status: a
        # source may have several rows (each retry adds one and leaves the old
        # one), and reporting all of them would list the same document
        # repeatedly. The inner ORDER BY must lead with the DISTINCT ON
        # expression; the outer one restores the "newest first" order the
        # caller sees.
        latest = (
            select(
                OutboxTaskRow.task_id,
                OutboxTaskRow.doc_id,
                OutboxTaskRow.source,
                OutboxTaskRow.status,
                OutboxTaskRow.error,
                OutboxTaskRow.created_at,
                OutboxTaskRow.updated_at,
            )
            .distinct(OutboxTaskRow.source)
            .order_by(OutboxTaskRow.source, OutboxTaskRow.updated_at.desc())
        )
        if sources:
            latest = latest.where(OutboxTaskRow.source.in_(sources))
        latest = latest.subquery()
        with self._sessions() as s:
            rows = s.execute(
                select(latest).order_by(latest.c.updated_at.desc())
            ).all()
        return [
            TaskSummary(
                task_id=r.task_id,
                doc_id=r.doc_id,
                source=r.source,
                status=TaskStatus(r.status),
                error=r.error or "",
                created_at=r.created_at,
                updated_at=r.updated_at,
            )
            for r in rows
        ]

    def requeue_failed(self, task_id: str) -> RequeueResult:
        # FOR UPDATE serializes concurrent requeues of the same task, so a
        # double-click produces one new row, not two. The source row is only
        # read: its status and error stay as the record of what happened. The
        # new row starts clean (no dispatched_at, no error) and
        # the dispatcher picks it up on its next pass.
        with self._sessions() as s:
            row = s.execute(
                select(OutboxTaskRow)
                .where(OutboxTaskRow.task_id == task_id)
                .with_for_update()
            ).scalar_one_or_none()
            if row is None:
                return RequeueResult(source_task_id=task_id, found=False)
            status = TaskStatus(row.status)
            if status is not TaskStatus.FAILED:
                return RequeueResult(
                    source_task_id=task_id, found=True, status=status
                )
            doc_id, source = row.doc_id, row.source
        new_task_id = uuid.uuid4().hex
        self.recreate_task(new_task_id, doc_id, source, requeued_from=task_id)
        return RequeueResult(
            source_task_id=task_id,
            found=True,
            status=status,
            new_task_id=new_task_id,
        )

    def set_failed(self, task_id: str, error: str) -> None:
        with self._sessions() as s:
            row = s.get(OutboxTaskRow, task_id)
            if row is not None:
                row.status = TaskStatus.FAILED.value
                row.error = error
                s.commit()

    def save_result(
        self, task_id: str, doc_id: str, source: str, doc_meta: DocMeta, chunks: list[Chunk]
    ) -> None:
        chunk_dicts = [
            {
                "chunk_id": c.chunk_id,
                "text": c.text,
                "page": c.metadata.page,
                "heading": c.metadata.heading,
                "chunk_type": c.metadata.chunk_type,
            }
            for c in chunks
        ]
        with self._sessions() as s:
            row = s.get(OutboxTaskRow, task_id)
            if row is None:
                row = OutboxTaskRow(
                    task_id=task_id,
                    doc_id=doc_id,
                    source=source,
                    status=TaskStatus.SUCCESS.value,
                    error=None,
                )
                s.add(row)
            row.status = TaskStatus.SUCCESS.value
            row.error = None
            row.total_pages = doc_meta.total_pages
            row.title = doc_meta.title
            row.warning = doc_meta.warning
            # Replace any prior chunks (idempotent on retry).
            s.query(ChunkRow).filter(ChunkRow.task_id == task_id).delete()
            for seq, c in enumerate(chunks):
                s.add(
                    ChunkRow(
                        task_id=task_id,
                        seq=seq,
                        chunk_id=c.chunk_id,
                        text=c.text,
                        page=c.metadata.page,
                        heading=c.metadata.heading,
                        chunk_type=c.metadata.chunk_type,
                    )
                )
            s.commit()

        # Cache only after the durable commit; a cache failure is not fatal.
        try:
            self._redis.set(
                _CHUNKS_KEY.format(task_id=task_id),
                json.dumps(chunk_dicts, ensure_ascii=False),
                ex=self._ttl,
            )
        except redis_lib.RedisError:
            _LOG.warning("chunks cache write failed for task %s", task_id, exc_info=True)

    # ---- read (Postgres for status, Redis for chunks) ----

    def get_snapshot(self, task_id: str) -> TaskSnapshot | None:
        with self._sessions() as s:
            row = s.get(OutboxTaskRow, task_id)
            if row is None:
                return None
            status = TaskStatus(row.status)
            if status != TaskStatus.SUCCESS:
                return TaskSnapshot(task_id=task_id, status=status, error=row.error or "")
            doc_meta = DocMeta(
                total_pages=row.total_pages or 0,
                title=row.title or "",
                warning=row.warning or "",
            )

        key = _CHUNKS_KEY.format(task_id=task_id)
        try:
            cached = self._redis.get(key)
        except redis_lib.RedisError:
            _LOG.warning("chunks cache read failed for task %s", task_id, exc_info=True)
            cached = None

        if cached is not None:
            chunks = [
                Chunk(
                    chunk_id=d["chunk_id"],
                    text=d["text"],
                    metadata=ChunkMetadata(
                        page=int(d.get("page", 0)),
                        heading=d.get("heading", ""),
                        chunk_type=d.get("chunk_type", "text"),
                    ),
                )
                for d in json.loads(cached)
            ]
        else:
            with self._sessions() as s:
                rows = (
                    s.query(ChunkRow)
                    .filter(ChunkRow.task_id == task_id)
                    .order_by(ChunkRow.seq)
                    .all()
                )
                chunks = [
                    Chunk(
                        chunk_id=r.chunk_id,
                        text=r.text,
                        metadata=ChunkMetadata(
                            page=r.page, heading=r.heading, chunk_type=r.chunk_type
                        ),
                    )
                    for r in rows
                ]
            try:
                self._redis.set(
                    key,
                    json.dumps(
                        [
                            {
                                "chunk_id": c.chunk_id,
                                "text": c.text,
                                "page": c.metadata.page,
                                "heading": c.metadata.heading,
                                "chunk_type": c.metadata.chunk_type,
                            }
                            for c in chunks
                        ],
                        ensure_ascii=False,
                    ),
                    ex=self._ttl,
                )
            except redis_lib.RedisError:
                _LOG.warning("chunks cache refill failed for task %s", task_id, exc_info=True)

        return TaskSnapshot(
            task_id=task_id, status=TaskStatus.SUCCESS, doc_meta=doc_meta, chunks=chunks
        )
