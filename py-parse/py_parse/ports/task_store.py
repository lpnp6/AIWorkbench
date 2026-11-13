"""Task/outbox persistence port.

PostgreSQL is the single source of truth: `create_task` inserts the outbox
row that a background dispatcher later publishes to the queue. Redis is only a
cache for chunks (successful results) — status is always read from the database,
so a cache miss or an eviction degrades nothing but latency.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from ..domain.models import Chunk, DocMeta


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass(frozen=True)
class DispatchRecord:
    """One row the dispatcher must publish to the queue."""

    task_id: str
    doc_id: str
    source: str


@dataclass
class TaskSnapshot:
    """A point-in-time view of a task, for the polling endpoint."""

    task_id: str
    status: TaskStatus
    error: str = ""
    doc_meta: DocMeta | None = None
    chunks: list[Chunk] = field(default_factory=list)


@dataclass(frozen=True)
class RequeueResult:
    """Outcome of asking to retry a FAILED task.

    A requeue never mutates the original row: the task record (its status, its
    error) is history and stays as it is. Instead a new
    PENDING outbox row is inserted for the same document, and the caller polls
    the *new* task id. `new_task_id` is None when nothing was inserted, and
    `status` tells the caller why (None = the source task is unknown).
    """

    source_task_id: str
    found: bool
    status: TaskStatus | None = None
    new_task_id: str | None = None


@dataclass(frozen=True)
class TaskSummary:
    """One task row, as reported by the task listing."""

    task_id: str
    doc_id: str
    source: str
    status: TaskStatus
    error: str
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True)
class CreateTaskResult:
    """Outcome of `create_task`.

    `created` is False when the source already has a task; `task_id` is then
    the id of the row that holds it, so the caller can report it.
    """

    created: bool
    task_id: str


class TaskStore(ABC):
    """Outbox persistence + chunks cache."""

    @abstractmethod
    def create_task(self, task_id: str, doc_id: str, source: str) -> CreateTaskResult:
        """Insert the PENDING row for a SubmitParse call, one task per source.

        The check and the insert must be one atomic step inside the store, not
        a read followed by a separate write in the caller: two concurrent
        submits of the same source would otherwise both see "no row yet". A
        FAILED task keeps its source taken — retries go through
        `recreate_task`, never through a second submit.
        """
        raise NotImplementedError

    @abstractmethod
    def recreate_task(
        self, task_id: str, doc_id: str, source: str, requeued_from: str
    ) -> None:
        """Insert a PENDING row for a retry of `requeued_from`'s document.

        Deliberately does NOT enforce source uniqueness: a retry is by
        definition a second task row for a source that already has one (the
        FAILED task it retries). `requeued_from` records the lineage.
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_undispatched(
        self, limit: int, stale_seconds: int
    ) -> list[DispatchRecord]:
        """Atomically mark and return rows the dispatcher must publish.

        Selects PENDING rows that are either never dispatched or whose
        `dispatched_at` is older than `stale_seconds` (a previous dispatcher
        died after marking but before publishing). Rows are marked in the same
        statement with ``FOR UPDATE SKIP LOCKED``, so concurrent dispatchers
        never take the same row.
        """
        raise NotImplementedError

    @abstractmethod
    def release_dispatch(self, task_id: str) -> None:
        """Undo the dispatch mark after a failed publish so the row is retried."""
        raise NotImplementedError

    @abstractmethod
    def list_tasks(self, sources: list[str] | None = None) -> list[TaskSummary]:
        """Return tasks of every status, newest first, at most one per source.

        A source can accumulate several rows: a retry creates a new task and
        leaves the original record in place. Listing them all would show the
        same document many times, so only the row carrying that source's most
        recent status is returned — that is the attempt whose lineage
        continues, and the one `requeue_failed` should be pointed at.

        `sources` filters the report to those source URIs; None or empty means
        every source. Callers use it as a per-source lookup: a source the
        report does not mention has no task yet and should be submitted.

        This is the operator's view and a caller's lookup, not a queue: task
        rows are never deleted, so a full listing only grows.
        """
        raise NotImplementedError

    @abstractmethod
    def requeue_failed(self, task_id: str) -> RequeueResult:
        """Insert a fresh PENDING task for a FAILED task's document.

        Read-only with respect to the source row: the FAILED record and its
        error are left untouched as history. Only a task whose
        current status is FAILED may be requeued — re-running a SUCCESS task
        would repeat an expensive parse for nothing, and re-running a PENDING
        or RUNNING one would put two workers on the same document. The lookup
        must be serialized (`FOR UPDATE`) so two concurrent requeues of the
        same task cannot both create a new row.
        """
        raise NotImplementedError

    @abstractmethod
    def fail_stale_running(self, timeout_seconds: int) -> int:
        """Fail RUNNING tasks whose claim is older than `timeout_seconds`.

        A worker that dies between its claim and its terminal write leaves the
        row RUNNING forever: nothing else transitions it, and a redelivered
        message becomes a no-op duplicate. This is the reaper that bounds that
        wait. Returns the number of rows reclaimed.
        """
        raise NotImplementedError

    @abstractmethod
    def try_claim(self, task_id: str) -> bool:
        """Atomically move a task PENDING → RUNNING; False if it was already claimed.

        The transition must be a single guarded write (``WHERE status =
        'pending'``) so concurrent or redelivered queue messages cannot start
        the same parse twice. A False result means the task is already running
        or finished: the caller must ack the message and return without doing
        any work. The same write stamps `claimed_at`, which is the lease the
        stale-RUNNING reaper measures against.
        """
        raise NotImplementedError

    @abstractmethod
    def set_failed(self, task_id: str, error: str) -> None:
        """Mark a task FAILED with an error message."""
        raise NotImplementedError

    @abstractmethod
    def save_result(
        self, task_id: str, doc_id: str, source: str, doc_meta: DocMeta, chunks: list[Chunk]
    ) -> None:
        """Persist a successful result: Postgres first, then the chunks cache."""
        raise NotImplementedError

    @abstractmethod
    def get_snapshot(self, task_id: str) -> TaskSnapshot | None:
        """Return the current snapshot, or None if the task is unknown."""
        raise NotImplementedError
