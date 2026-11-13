"""Runtime configuration, loaded from environment variables.

Per the service contract, a `ParseRequest` carries only *location* (doc_id +
source). Connection config (MinerU endpoint, storage credentials) lives here,
injected at adapter-construction time — never in the request.

Every setting has a localhost-friendly default; the Docker compose overrides
the ones that point at sibling services.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def _bool_env(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Settings:
    # gRPC server
    grpc_host: str = "0.0.0.0"
    grpc_port: int = 50051
    grpc_max_workers: int = 10

    # MinerU (document extraction) — a separate HTTP service
    mineru_url: str = "http://localhost:8000"
    # Overall deadline for one document (create → upload → parse → download).
    # Not a per-request HTTP timeout; Celery's soft/hard limits sit above it.
    mineru_timeout: float = 300.0

    # Async task infrastructure: Celery broker/result backend, Postgres as the
    # outbox source of truth, and a separate Redis used only as a chunks cache.
    redis_url: str = "redis://localhost:6379/0"
    postgres_dsn: str = "postgresql+psycopg://postgres:postgres@localhost:5432/pyparse"
    cache_redis_url: str = "redis://localhost:6380/0"
    result_ttl_seconds: int = 3600

    # Outbox dispatcher: how often to scan, how many rows per pass, and how long
    # a dispatch mark may stay unclaimed before the row is published again.
    outbox_poll_interval: float = 1.0
    outbox_batch_size: int = 50
    outbox_stale_seconds: int = 300
    # A RUNNING row whose claim is older than this is marked FAILED by the
    # dispatcher: a worker killed mid-parse never writes a terminal state, and
    # a redelivered message only turns into a no-op duplicate. Must exceed the
    # longest legitimate parse (MINERU_TIMEOUT + slack), otherwise a slow
    # document is failed while its worker is still alive.
    running_timeout_seconds: int = 3600

    # Storage backends. A scheme's reader is only registered when its config
    # is present, so unused backends don't require their SDK clients at all.
    minio_endpoint: str | None = None
    minio_access_key: str | None = None
    minio_secret_key: str | None = None
    minio_secure: bool = False

    s3_endpoint_url: str | None = None
    s3_region: str | None = None
    s3_access_key: str | None = None
    s3_secret_key: str | None = None

    @classmethod
    def from_env(cls, environ: os._Environ | None = None) -> "Settings":
        env = environ or os.environ
        get = env.get

        def opt(name: str) -> str | None:
            v = get(name)
            return v if v else None

        return cls(
            grpc_host=get("GRPC_HOST", "0.0.0.0"),
            grpc_port=_int_env("GRPC_PORT", 50051),
            grpc_max_workers=_int_env("GRPC_MAX_WORKERS", 10),
            mineru_url=get("MINERU_URL", "http://localhost:8000"),
            mineru_timeout=_float_env("MINERU_TIMEOUT", 300.0),
            redis_url=get("REDIS_URL", "redis://localhost:6379/0"),
            postgres_dsn=get(
                "POSTGRES_DSN",
                "postgresql+psycopg://postgres:postgres@localhost:5432/pyparse",
            ),
            cache_redis_url=get("CACHE_REDIS_URL", "redis://localhost:6380/0"),
            result_ttl_seconds=_int_env("RESULT_TTL_SECONDS", 3600),
            outbox_poll_interval=_float_env("OUTBOX_POLL_INTERVAL", 1.0),
            outbox_batch_size=_int_env("OUTBOX_BATCH_SIZE", 50),
            outbox_stale_seconds=_int_env("OUTBOX_STALE_SECONDS", 300),
            running_timeout_seconds=_int_env("RUNNING_TIMEOUT_SECONDS", 3600),
            minio_endpoint=opt("MINIO_ENDPOINT"),
            minio_access_key=opt("MINIO_ACCESS_KEY"),
            minio_secret_key=opt("MINIO_SECRET_KEY"),
            minio_secure=_bool_env("MINIO_SECURE", False),
            s3_endpoint_url=opt("S3_ENDPOINT_URL"),
            s3_region=opt("S3_REGION"),
            s3_access_key=opt("S3_ACCESS_KEY"),
            s3_secret_key=opt("S3_SECRET_KEY"),
        )
