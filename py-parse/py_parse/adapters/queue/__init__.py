"""Async queue adapters: Celery app, worker task and `TaskQueue` implementation."""

from .celery_app import celery_app, make_celery
from .celery_queue import CeleryTaskQueue
from .tasks import parse_document

__all__ = [
    "CeleryTaskQueue",
    "celery_app",
    "make_celery",
    "parse_document",
]
