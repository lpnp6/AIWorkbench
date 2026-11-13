"""Outbox dispatcher: publishes PENDING task rows to the Celery queue.

`SubmitParse` only inserts the outbox row; this process closes the gap to
Celery. The row's status intentionally mirrors Celery's state:

    PENDING (row)  → published, no Celery result key yet (Celery: PENDING)
    RUNNING (row)  ← worker claim                       (Celery: STARTED)
    SUCCESS/FAILED (row) ← worker                       (Celery: SUCCESS/FAILURE)

Publishing is at-least-once: a dispatcher can die after marking but before the
broker accepted the message, and stale marks are re-published. The worker's
Postgres claim (``WHERE status = 'pending'``) is what makes execution happen at
most once regardless of duplicate publishes. Each publish still gets a fresh
queue message id, so a duplicate cannot overwrite the result record of the
publish that did the work.

The dispatcher also reaps: a RUNNING row whose `claimed_at` is older than
`RUNNING_TIMEOUT_SECONDS` is marked FAILED. A worker killed mid-parse never
writes a terminal state and a redelivered message is only a no-op duplicate,
so without this the row would stay RUNNING forever.

Run with: python -m py_parse.adapters.queue.dispatcher
"""
from __future__ import annotations

import logging
import time

from ...config import Settings
from ...factory import build_store
from .celery_queue import CeleryTaskQueue

_LOG = logging.getLogger("py_parse.outbox")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = Settings.from_env()
    store = build_store(settings)
    queue = CeleryTaskQueue()
    _LOG.info(
        "outbox dispatcher started (batch=%d, poll=%.1fs, stale=%ds)",
        settings.outbox_batch_size,
        settings.outbox_poll_interval,
        settings.outbox_stale_seconds,
    )
    while True:
        try:
            # Reap first: a worker killed between its claim and its terminal
            # write leaves the row RUNNING forever, and nothing else would ever
            # transition it. Failing it here makes the loss visible and lets
            # RequeueParse start a fresh task for the document.
            reclaimed = store.fail_stale_running(settings.running_timeout_seconds)
            if reclaimed:
                _LOG.warning(
                    "failed %d task(s) still RUNNING after %ds without a terminal write",
                    reclaimed,
                    settings.running_timeout_seconds,
                )
            rows = store.fetch_undispatched(
                settings.outbox_batch_size, settings.outbox_stale_seconds
            )
            for rec in rows:
                try:
                    queue.enqueue(rec.task_id, rec.doc_id, rec.source)
                except Exception:  # noqa: BLE001 - keep the row for a retry
                    _LOG.exception(
                        "failed to publish task %s; releasing", rec.task_id
                    )
                    store.release_dispatch(rec.task_id)
            if not rows:
                time.sleep(settings.outbox_poll_interval)
        except KeyboardInterrupt:
            _LOG.info("outbox dispatcher stopped")
            return
        except Exception:  # noqa: BLE001 - the loop must survive transient DB errors
            _LOG.exception(
                "outbox dispatch pass failed; retrying in %.1fs",
                settings.outbox_poll_interval,
            )
            time.sleep(settings.outbox_poll_interval)


if __name__ == "__main__":
    main()
