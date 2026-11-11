"""Celery application factory (infrastructure adapter).

The broker and result backend are both Redis (status is additionally mirrored
into Redis + PostgreSQL by the task store; the Celery result backend is only
used for Celery's own task tracking).

Start a worker with:
    celery -A py_parse.adapters.queue.celery_app worker --loglevel=info -c 2
"""
from __future__ import annotations

from celery import Celery

from ...config import Settings


def make_celery(settings: Settings) -> Celery:
    app = Celery(
        "py_parse",
        broker=settings.redis_url,
        backend=settings.redis_url,
    )
    app.conf.update(
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        task_track_started=True,
        include=["py_parse.adapters.queue.tasks"],
        worker_prefetch_multiplier=1,  # don't prefetch tasks (1.0 = 1 task at a time)
        task_acks_late=True,  # ack tasks after completion (not before)
        # The client's job deadline (MINERU_TIMEOUT) must fire first so it can
        # cancel the MinerU task and record a clean failure; Celery's limits are
        # only a backstop against a hung worker.
        task_soft_time_limit=int(settings.mineru_timeout) + 120,
        task_time_limit=int(settings.mineru_timeout) + 180,
        # A late-acked task stays invisible to other workers for this long. It
        # must exceed the longest task, otherwise Redis redelivers and the same
        # document gets parsed twice.
        broker_transport_options={
            "visibility_timeout": max(3600, int(settings.mineru_timeout) + 600)
        },
    )
    return app


# Module-level app: `celery -A` imports this module and finds `celery_app`.
celery_app = make_celery(Settings.from_env())
