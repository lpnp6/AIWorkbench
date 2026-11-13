"""Celery task: a thin adapter that triggers the `run_parse_task` use case.

The actual parse orchestration lives in `domain.parse_task` (business logic);
this module only wires the concrete dependencies and registers the task with
Celery. The gRPC server never imports this module.

The extractor is cached per worker process: one worker handles one task at a
time, so its MinerU client (and HTTP connection pool) is reused across tasks.
The PID check matters because Celery's prefork pool imports this module before
forking; a client created in the parent would share sockets with the child.
"""
from __future__ import annotations

import logging
import os
import random

from ...config import Settings
from ...domain.parse_task import run_parse_task
from ...domain.parser import DocumentParser
from ...factory import build_extractor, build_registry, build_store
from ...ports.document_extractor import DocumentExtractor
from ...ports.task_store import TaskStatus
from .celery_app import celery_app

_LOG = logging.getLogger("py_parse.tasks")

_extractor: DocumentExtractor | None = None
_extractor_pid: int | None = None


@celery_app.task(name="py_parse.parse_document", bind=True, max_retries=None)
def parse_document(
    self, task_id: str, doc_id: str, source: str
) -> dict[str, object]:
    global _extractor, _extractor_pid
    # The business task id travels in the message body (Celery's own message id
    # is per publish and unrelated to it).
    settings = Settings.from_env()
    pid = os.getpid()
    if _extractor is None or _extractor_pid != pid:
        _extractor = build_extractor(settings)
        _extractor_pid = pid
    # Health-gate before claiming. If the engine is down, `try_claim` would
    # park the row RUNNING and hold this worker for the whole MINERU_TIMEOUT;
    # retrying instead keeps the row PENDING (the dispatcher can still
    # re-publish it) and frees the slot while the outage lasts.
    if not _extractor.health():
        delay = min(15.0 * 2.0 ** min(self.request.retries, 3), 120.0)
        # Same equal jitter as the client's create loop: every deferred
        # worker would otherwise wake in lockstep.
        countdown = delay / 2 + random.uniform(0, delay / 2)
        _LOG.info(
            "mineru-api unhealthy; deferring task %s (retry %d) for %.0fs",
            task_id,
            self.request.retries,
            countdown,
        )
        raise self.retry(countdown=countdown)
    store = build_store(settings)
    output = run_parse_task(
        task_id,
        doc_id,
        source,
        build_registry(settings),
        DocumentParser(_extractor),
        store,
    )
    if output is None:
        # Another message claimed the task first; the outbox row already says
        # what happened (RUNNING or a terminal state), so just ack.
        return {
            "task_id": task_id,
            "doc_id": doc_id,
            "status": "duplicate",
        }
    # Returning metadata is what writes Celery's SUCCESS result for this
    # message; chunks stay in Postgres/Redis, so the backend record stays small.
    return {
        "task_id": task_id,
        "doc_id": doc_id,
        "status": TaskStatus.SUCCESS.value,
        "chunk_count": len(output.chunks),
        "total_pages": output.doc_meta.total_pages,
        "title": output.doc_meta.title,
    }
