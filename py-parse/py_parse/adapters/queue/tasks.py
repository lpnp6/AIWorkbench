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

import os

from ...config import Settings
from ...domain.parse_task import run_parse_task
from ...domain.parser import DocumentParser
from ...factory import build_extractor, build_registry, build_store
from ...ports.document_extractor import DocumentExtractor
from .celery_app import celery_app

_extractor: DocumentExtractor | None = None
_extractor_pid: int | None = None


def get_extractor(settings: Settings) -> DocumentExtractor:
    """Return this worker process's extractor, creating it on first use."""
    global _extractor, _extractor_pid
    pid = os.getpid()
    if _extractor is None or _extractor_pid != pid:
        _extractor = build_extractor(settings)
        _extractor_pid = pid
    return _extractor


@celery_app.task(name="py_parse.parse_document", bind=True)
def parse_document(self, task_id: str, doc_id: str, source: str) -> None:
    settings = Settings.from_env()
    registry = build_registry(settings)
    parser = DocumentParser(get_extractor(settings))
    store = build_store(settings)
    run_parse_task(task_id, doc_id, source, registry, parser, store)
