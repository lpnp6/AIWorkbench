"""Celery-backed `TaskQueue` adapter.

Implements the `TaskQueue` port by enqueuing a `parse_document` task. A fresh
message id is generated per publish, so each publish keeps its own result
record; the business `task_id` travels in the message body and is what the
worker uses for the atomic claim.
"""
from __future__ import annotations

import uuid

from ...ports.task_queue import TaskQueue
from .tasks import parse_document


class CeleryTaskQueue(TaskQueue):
    def enqueue(self, task_id: str, doc_id: str, source: str) -> None:
        parse_document.apply_async(
            args=[task_id, doc_id, source], task_id=uuid.uuid4().hex
        )
