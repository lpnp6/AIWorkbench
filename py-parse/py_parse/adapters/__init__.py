"""Adapters: concrete implementations of the ports (replaceable).

Grouped by infrastructure concern:

    extractor/         document extraction adapters (MinerU V1 Tasks API)
    source_reader/     storage backends (file/minio/s3) + scheme registry
    db/                SQLAlchemy models and the durable task store
    queue/             Celery app, worker task and TaskQueue adapter

Connection config is injected at construction time, never in the request.
"""

from .db import RedisPostgresTaskStore
from .extractor import MineruClient
from .queue import CeleryTaskQueue
from .source_reader import (
    FileSourceReader,
    MinioSourceReader,
    S3SourceReader,
    SourceReaderRegistry,
)

__all__ = [
    "CeleryTaskQueue",
    "FileSourceReader",
    "MineruClient",
    "MinioSourceReader",
    "RedisPostgresTaskStore",
    "S3SourceReader",
    "SourceReaderRegistry",
]
