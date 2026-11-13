"""SQLAlchemy models + engine for the outbox task store (PostgreSQL).

`outbox_tasks` is both the domain task record and the outbox: a PENDING row is
the event that the dispatcher must publish to Celery. `chunks` holds the parsed
chunks; Redis only caches them.

Status values mirror Celery's execution states so the two sides stay in sync:

    PENDING  ↔ queued (no Celery result key yet)
    RUNNING  ↔ STARTED
    SUCCESS  ↔ SUCCESS
    FAILED   ↔ FAILURE
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Index,
    String,
    Text,
    create_engine,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


class Base(DeclarativeBase):
    pass


class OutboxTaskRow(Base):
    __tablename__ = "outbox_tasks"
    # The dispatcher scans PENDING rows by (dispatched_at, created_at); the
    # worker claims by task_id alone.
    __table_args__ = (Index("ix_outbox_dispatch", "status", "dispatched_at", "created_at"),)

    task_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    doc_id: Mapped[str] = mapped_column(String(256))
    source: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(16))
    error: Mapped[str | None] = mapped_column(Text)
    total_pages: Mapped[int | None] = mapped_column(nullable=True)
    title: Mapped[str | None] = mapped_column(Text)
    warning: Mapped[str | None] = mapped_column(Text)
    # NULL = not published yet (or publish failed and was rolled back). Stale
    # non-NULL values are re-published; the worker's atomic claim dedups them.
    dispatched_at: Mapped[datetime | None] = mapped_column(DateTime)
    # Set by the PENDING -> RUNNING claim. The dispatcher fails a RUNNING row
    # whose claim is older than the running timeout: a worker killed between
    # the claim and its terminal write would otherwise park the row forever.
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime)
    # Set when this row is a retry of an earlier task. Requeueing never touches
    # the original row, so the lineage lives on the new one.
    requeued_from: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )


class ChunkRow(Base):
    __tablename__ = "chunks"
    __table_args__ = (Index("ix_chunks_task_seq", "task_id", "seq"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(
        ForeignKey("outbox_tasks.task_id", ondelete="CASCADE"), index=True
    )
    seq: Mapped[int] = mapped_column()
    chunk_id: Mapped[str] = mapped_column(String(256))
    text: Mapped[str] = mapped_column(Text)
    page: Mapped[int] = mapped_column()
    heading: Mapped[str] = mapped_column(Text)
    chunk_type: Mapped[str] = mapped_column(String(16))


def create_engine_from_dsn(dsn: str):
    """Create a sync engine (psycopg2). `pool_pre_ping` guards stale connections."""
    return create_engine(dsn, pool_pre_ping=True, future=True)


def init_db(engine) -> None:
    """Create tables if they don't exist (idempotent). Alembic is the follow-up
    once the schema stabilizes."""
    Base.metadata.create_all(engine)


def make_session_factory(engine):
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
