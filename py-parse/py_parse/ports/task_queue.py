"""Task queue port.

Abstracts "enqueue a parse task for a worker to execute" behind an interface.
The concrete queue (Celery, RQ, a cloud queue, …) is a replaceable adapter; the
gRPC server only depends on this port, never on a specific queue engine.
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class TaskQueue(ABC):
    """Submit parse tasks to a worker pool."""

    @abstractmethod
    def enqueue(self, task_id: str, doc_id: str, source: str) -> None:
        """Enqueue a parse task for execution.

        `task_id` is the caller-assigned business id and travels in the message
        body; the adapter generates a fresh message id per publish, so a
        duplicate publish keeps its own result record and can never overwrite
        the result of the publish that did the work.
        """
        raise NotImplementedError
