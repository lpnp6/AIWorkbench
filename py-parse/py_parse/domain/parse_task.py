"""Parse-task use case: the read → extract → chunk → persist orchestration.

This is the business logic of "parse a document for a task" — what a worker
actually does. It depends only on ports (SourceReader, DocumentExtractor,
TaskStore) and the domain parser, never on concrete adapters. The trigger (a
Celery task) and the concrete dependencies are supplied by the caller.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from urllib.parse import unquote, urlparse

from ..ports.document_extractor import DocumentExtractError
from ..ports.source_reader import SourceReadError, UnsupportedSchemeError
from ..ports.task_store import TaskStore
from .parser import DocumentParser

if TYPE_CHECKING:
    from ..adapters.source_reader import SourceReaderRegistry

LOG = logging.getLogger("py_parse.parse_task")


def run_parse_task(
    task_id: str,
    doc_id: str,
    source: str,
    registry: "SourceReaderRegistry",
    parser: DocumentParser,
    store: TaskStore,
) -> None:
    """Execute one parse task end-to-end.

    Atomically claims the task (PENDING → RUNNING), reads the source, extracts,
    chunks, and persists the result. On a domain error it records the failure
    and re-raises (so the queue engine sees the task as failed too).

    If the claim fails, the task is already running or finished — a redelivered
    or duplicated queue message. Parsing is expensive, so this is a no-op: ack
    and return. The trade-off is that a worker killed mid-parse leaves the task
    RUNNING until an operator (or a future reaper) resolves it; that is
    deliberate, because re-running a half-finished parse is not free.
    """
    if not store.try_claim(task_id):
        LOG.info(
            "task %s already claimed or finished; skipping duplicate delivery",
            task_id,
        )
        return
    try:
        reader = registry.resolve(source)   # raises UnsupportedSchemeError
        stream = reader.read(source)        # raises SourceReadError
        with stream:
            output = parser.parse(stream, doc_id=doc_id, filename=_source_filename(source))
        store.save_result(task_id, doc_id, source, output.doc_meta, output.chunks)
    except (UnsupportedSchemeError, SourceReadError, DocumentExtractError) as e:
        store.set_failed(task_id, str(e))
        raise


def _source_filename(source: str) -> str:
    """Basename of the source URI; extractors use its extension to pick a parser."""
    path = urlparse(source).path if "://" in source else source
    name = unquote(path.replace("\\", "/").rsplit("/", 1)[-1]).strip()
    return name or "document.pdf"
