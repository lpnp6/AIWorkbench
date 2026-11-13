#!/usr/bin/env python
"""Batch-parse a local directory through the ParseService gRPC API.

Before submitting anything the script asks `ListTasks` about every source it is
about to parse, then, per file, does exactly one of:

    no task yet      SubmitParse                 (action=submitted)
    newest SUCCESS   skip, nothing to do         (action=skipped)
    newest FAILED    RequeueParse → new task     (action=requeued)
    newest PENDING/RUNNING
                     attach to that task         (action=attached)

So re-running the same directory only parses what is missing, retries what
failed, and joins what is still in flight — it never resubmits a source that
already has a task (which `SubmitParse` would reject with ALREADY_EXISTS).

Each file gets its own worker (the worker count equals the number of selected
files); that worker owns the file's task and polls `GetResult` until it reaches
a terminal state. The program does not return while any task is still pending
or running — there is no global duration limit and no per-task timeout.

Per task it records:

    submit_s  SubmitParse/RequeueParse call latency
    queue_s   task call response -> first observed RUNNING (time queued)
    exec_s    first observed RUNNING -> terminal state (parse execution)
    total_s   task call response -> terminal state

RUNNING is observed by polling, so the resolution is the poll interval. A
worker holds RUNNING for the whole parse, so the queue timing is reliable;
tasks that finish between two polls have no observed RUNNING and get null
queue_s/exec_s (their total_s is still recorded).

Output layout (--out-dir):

    results/<index>_<name>.json   full chunk payload per successful file
    files.jsonl                   one row per file: timings + outcome
    summary.json                  aggregate outcome and overall throughput
    run.log                       human-readable progress log

Why staging?  The Celery workers read `file://` sources from *their own*
container filesystem. Unless the workers already bind-mount the corpus, the
selected files are streamed into every running `celery-worker` container with
`docker exec ... tar` before submission. Nothing in compose changes.

Examples:

    # smoke test: 5 PDFs
    python scripts/benchmark_parse.py --root D:/MMLongBench-Doc/documents --ext .pdf --limit 5

    # everything, resumable
    python scripts/benchmark_parse.py --root D:/MMLongBench-Doc/documents --out-dir out/bench-full --resume

    # files are already present in the workers (no staging)
    python scripts/benchmark_parse.py --root D:/MMLongBench-Doc/documents --stage none \
        --container-root /data/books

Why threads rather than processes?  Polling is I/O-bound, and Windows cannot
run this many worker processes from one manager anyway: ProcessPoolExecutor
caps at 61 workers, and multiprocessing.Pool fails beyond 63 wait handles
(`ValueError: need at most 63 handles`). Threads give every file its own
concurrent worker without that ceiling; the GIL is released while gRPC waits.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import re
import subprocess
import sys
import tarfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from fnmatch import fnmatch
from pathlib import Path
from typing import Any
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import grpc  # noqa: E402

from py_parse.generated import (  # noqa: E402
    GetResultRequest,
    ListTasksRequest,
    ParseRequest,
    ParseServiceStub,
    RequeueParseRequest,
    TaskStatus,
)

LOG = logging.getLogger("bench")

# outbox_tasks.doc_id is varchar(256); keep a safe margin.
MAX_DOC_ID_LEN = 240
# Result payloads can be large (many chunks); the server's default client-side
# receive limit is 4 MiB, which is easy to exceed on a book-length document.
GRPC_MAX_MESSAGE_BYTES = 512 * 1024 * 1024
RETRYABLE = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}
SUBMIT_ATTEMPTS = 5

SYSTEM_FILES = {"thumbs.db", "desktop.ini", ".ds_store"}
_BAD_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


# --------------------------------------------------------------------------- #
# small helpers
# --------------------------------------------------------------------------- #
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def _fmt(value: float | None, digits: int = 1) -> str:
    return "-" if value is None else f"{value:.{digits}f}"


def _round(value: float | None, digits: int = 1) -> float | None:
    return None if value is None else round(value, digits)


def _safe_size(path: Path) -> int | None:
    try:
        return path.stat().st_size
    except OSError:
        return None


def _percentile(sorted_values: list[float], q: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    pos = (len(sorted_values) - 1) * q
    lo, hi = math.floor(pos), math.ceil(pos)
    if lo == hi:
        return sorted_values[int(pos)]
    return sorted_values[lo] * (hi - pos) + sorted_values[hi] * (pos - lo)


def _summary_stats(values: list[float]) -> dict[str, Any]:
    ordered = sorted(values)
    if not ordered:
        return {"count": 0, "mean": None, "p50": None, "p90": None, "p95": None, "max": None}
    return {
        "count": len(ordered),
        "mean": round(sum(ordered) / len(ordered), 3),
        "p50": _round(_percentile(ordered, 0.50), 3),
        "p90": _round(_percentile(ordered, 0.90), 3),
        "p95": _round(_percentile(ordered, 0.95), 3),
        "max": round(ordered[-1], 3),
    }


def _make_doc_id(rel: str, max_len: int = MAX_DOC_ID_LEN) -> str:
    """Fit a relative path into outbox_tasks.doc_id (varchar(256)).

    Short paths are used verbatim; long ones keep a hash prefix plus the tail
    (the filename is what matters when reading server logs).
    """
    if len(rel) <= max_len:
        return rel
    digest = hashlib.sha1(rel.encode("utf-8")).hexdigest()[:10]
    tail = rel[-(max_len - len(digest) - 1):]
    return f"{digest}:{tail}"


def _source_uri(rel: str, container_root: str) -> str:
    return "file://" + quote(f"{container_root}/{rel}", safe="/")


def lookup_existing(
    addr: str, sources: list[str], batch: int = 500
) -> dict[str, Any]:
    """Map source URI → that source's newest task summary.

    One `ListTasks` call per batch of sources (the request carries the whole
    list; batching only keeps a huge corpus from producing one giant message).
    A source missing from the result has no task and must be submitted.
    """
    existing: dict[str, Any] = {}
    channel = grpc.insecure_channel(
        addr,
        options=[("grpc.max_receive_message_length", GRPC_MAX_MESSAGE_BYTES)],
    )
    stub = ParseServiceStub(channel)
    try:
        for start in range(0, len(sources), batch):
            chunk = sources[start : start + batch]
            for attempt in range(SUBMIT_ATTEMPTS):
                try:
                    response = stub.ListTasks(
                        ListTasksRequest(sources=chunk), timeout=60
                    )
                    break
                except grpc.RpcError as exc:
                    if exc.code() in RETRYABLE and attempt < SUBMIT_ATTEMPTS - 1:
                        time.sleep(min(5.0, 2.0**attempt))
                        continue
                    raise
            for task in response.tasks:
                existing[task.source] = task
    finally:
        channel.close()
    return existing


def _safe_stem(rel: str) -> str:
    stem = _BAD_FILENAME_CHARS.sub("_", rel.replace("/", "__")).strip(" .")
    if len(stem) > 120:
        digest = hashlib.sha1(rel.encode("utf-8")).hexdigest()[:10]
        stem = stem[:100] + "_" + digest
    return stem or "file"


def setup_logging(log_path: Path) -> None:
    try:  # never crash on a legacy console codepage
        sys.stdout.reconfigure(errors="replace")  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        pass
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(fmt)
    root.addHandler(stream)
    root.addHandler(file_handler)


# --------------------------------------------------------------------------- #
# docker helpers — all runtime commands live here, not in compose
# --------------------------------------------------------------------------- #
def _docker(*args: str, timeout: float = 120.0) -> str:
    cmd = ["docker", *args]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("docker CLI not found on PATH") from exc
    if proc.returncode != 0:
        raise RuntimeError(
            f"`{' '.join(cmd)}` failed (rc={proc.returncode}): {proc.stderr.strip()}"
        )
    return proc.stdout


def discover_workers(project: str, service: str) -> list[str]:
    """Running containers of one compose service (one per replica)."""
    out = _docker(
        "ps",
        "-q",
        "--filter",
        "status=running",
        "--filter",
        f"label=com.docker.compose.project={project}",
        "--filter",
        f"label=com.docker.compose.service={service}",
    )
    return [line.strip() for line in out.splitlines() if line.strip()]


def _host_path(src: str) -> Path:
    """Normalise a docker-inspect mount source to a host path.

    Docker Desktop sometimes reports WSL2-style sources
    (/run/desktop/mnt/host/d/...) instead of the Windows path.
    """
    prefix = "/run/desktop/mnt/host/"
    if os.name == "nt" and src.startswith(prefix):
        drive, _, tail = src[len(prefix):].partition("/")
        return Path(f"{drive.upper()}:\\" + tail.replace("/", "\\"))
    return Path(src)


def detect_mounted_root(containers: list[str], root: Path) -> str | None:
    """Return the container path of an existing bind mount covering `root`.

    If every worker already bind-mounts the host directory (or an ancestor of
    it), no staging is needed: return the path to use in file:// URIs.
    """
    root_norm = os.path.normcase(str(root))
    targets: list[str] = []

    for cid in containers:
        raw = _docker("inspect", cid, "--format", "{{json .Mounts}}", timeout=60)
        try:
            mounts = json.loads(raw or "[]")
        except json.JSONDecodeError:
            return None
        target = None
        for mount in mounts:
            if mount.get("Type") != "bind":
                continue
            src, dst = mount.get("Source"), mount.get("Destination")
            if not src or not dst:
                continue
            try:
                src_path = _host_path(src).resolve()
            except OSError:
                continue
            src_norm = os.path.normcase(str(src_path))
            if root_norm == src_norm:
                target = dst
                break
            if root_norm.startswith(src_norm + os.sep):
                rel = root.relative_to(src_path).as_posix()
                target = dst.rstrip("/") + "/" + rel
                break
        if target is None:
            return None  # one worker without the mount is enough to need staging
        targets.append(target)

    if not targets or len({os.path.normcase(t) for t in targets}) != 1:
        return None
    return targets[0]


def stage_into_workers(
    containers: list[str], files: list[Path], root: Path, croot: str, total_bytes: int
) -> None:
    """Stream the selected files into every worker at `croot/<rel>`.

    One tar stream per container: the workers pick tasks from a shared queue,
    so any replica may be the one reading a given source.
    """
    LOG.info(
        "staging %d file(s) (%.1f MiB) into %d worker container(s) -> %s",
        len(files),
        total_bytes / 2**20,
        len(containers),
        croot,
    )
    for cid in containers:
        short = cid[:12]
        _docker("exec", cid, "mkdir", "-p", croot, timeout=60)
        try:
            df = _docker("exec", cid, "df", "-h", croot, timeout=30).splitlines()
            if len(df) > 1:
                LOG.info("  worker %s disk: %s", short, " ".join(df[1].split()))
        except RuntimeError:
            pass

        proc = subprocess.Popen(
            ["docker", "exec", "-i", cid, "tar", "-xf", "-", "-C", croot],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        try:
            assert proc.stdin is not None
            with tarfile.open(fileobj=proc.stdin, mode="w|") as tar:
                for i, path in enumerate(files, 1):
                    tar.add(str(path), arcname=path.relative_to(root).as_posix())
                    if i % 25 == 0 or i == len(files):
                        LOG.info("  worker %s: archived %d/%d", short, i, len(files))
        except BrokenPipeError:
            pass  # tar exited early; reported through the exit code below
        finally:
            try:
                if proc.stdin is not None:
                    proc.stdin.close()
            except OSError:
                pass

        stderr = proc.stderr.read() if proc.stderr else b""
        rc = proc.wait()
        if rc != 0:
            raise RuntimeError(
                f"tar staging into {cid} failed (rc={rc}): "
                f"{stderr.decode('utf-8', 'replace').strip()}"
            )
        LOG.info("  worker %s: staged %d file(s)", short, len(files))

    probe = files[0].relative_to(root).as_posix()
    for cid in containers:
        _docker("exec", cid, "test", "-f", f"{croot}/{probe}", timeout=60)
    LOG.info(
        "staging verified in %s",
        ", ".join(c[:12] for c in containers),
    )


def cleanup_staged(containers: list[str], croot: str) -> None:
    for cid in containers:
        try:
            _docker("exec", cid, "rm", "-rf", croot, timeout=120)
            LOG.info("removed staged files from %s", cid[:12])
        except RuntimeError as exc:
            LOG.warning("cleanup failed for %s: %s", cid[:12], exc)


# --------------------------------------------------------------------------- #
# one file = one worker: submit, poll to terminal, persist the result
# --------------------------------------------------------------------------- #
class TaskFailed(Exception):
    pass


def _run_file(idx: int, path_str: str, cfg: dict[str, Any]) -> dict[str, Any]:
    """Worker entry point: take one file from lookup to a terminal state.

    `cfg["existing"]` holds the newest task per source from the pre-submit
    `ListTasks`: a SUCCESS is skipped, a FAILED is requeued as a new task, a
    PENDING/RUNNING is attached to, and a missing entry is submitted.

    Never gives up on a retryable transport error or on PENDING/RUNNING: the
    benchmark's contract is that the worker ends only when the task does.
    """
    path = Path(path_str)
    rel = path.relative_to(Path(cfg["root"])).as_posix()
    doc_id = _make_doc_id(rel)
    source = _source_uri(rel, cfg["container_root"])
    results_dir = Path(cfg["results_dir"])

    row: dict[str, Any] = {
        "run_id": cfg["run_id"],
        "index": idx,
        "doc_id": doc_id,
        "path": rel,
        "task_id": "",
        "source": source,
        "action": "",
        "status": "failed",
        "error": "",
        "submitted_at": None,
        "started_at": None,
        "finished_at": None,
        "submit_s": None,
        "queue_s": None,
        "exec_s": None,
        "total_s": None,
        "polls": 0,
        "size_bytes": _safe_size(path),
        "chunk_count": 0,
        "total_pages": 0,
        "title": "",
        "warning": "",
        "result_file": None,
    }

    existing = cfg["existing"].get(source)
    if existing is not None and existing.status == TaskStatus.TASK_STATUS_SUCCESS:
        # Already parsed by an earlier call: resubmitting is rejected by
        # design (one task per source), and there is nothing left to do.
        row["task_id"] = existing.task_id
        row["action"] = "skipped"
        row["status"] = "skipped"
        row["finished_at"] = utc_now()
        return row

    channel = grpc.insecure_channel(
        cfg["addr"],
        options=[("grpc.max_receive_message_length", GRPC_MAX_MESSAGE_BYTES)],
    )
    stub = ParseServiceStub(channel)
    t0 = time.monotonic()
    t_submitted: float | None = None
    t_done: float | None = None
    started: float | None = None

    try:
        if existing is not None and existing.status == TaskStatus.TASK_STATUS_FAILED:
            # Retry: requeue inserts a NEW task for the same source, leaving
            # the failed record as history. Poll the new id.
            row["action"] = "requeued"
            for attempt in range(SUBMIT_ATTEMPTS):
                try:
                    requeued = stub.RequeueParse(
                        RequeueParseRequest(task_id=existing.task_id), timeout=30
                    )
                    row["task_id"] = requeued.task_id
                    break
                except grpc.RpcError as exc:
                    if exc.code() in RETRYABLE and attempt < SUBMIT_ATTEMPTS - 1:
                        time.sleep(min(cfg["poll_max_interval"], 2.0**attempt))
                        continue
                    raise
        elif existing is not None:
            # PENDING or RUNNING: the document is already in the pipeline, so
            # do not create a second task — just wait for this one.
            row["action"] = "attached"
            row["task_id"] = existing.task_id
        else:
            row["action"] = "submitted"
            for attempt in range(SUBMIT_ATTEMPTS):
                try:
                    sub = stub.SubmitParse(
                        ParseRequest(doc_id=doc_id, source=source), timeout=30
                    )
                    row["task_id"] = sub.task_id
                    break
                except grpc.RpcError as exc:
                    if exc.code() in RETRYABLE and attempt < SUBMIT_ATTEMPTS - 1:
                        time.sleep(min(cfg["poll_max_interval"], 2.0**attempt))
                        continue
                    raise

        t_submitted = time.monotonic()
        row["submitted_at"] = utc_now()
        row["submit_s"] = round(t_submitted - t0, 4)

        interval = cfg["poll_interval"]
        success_resp = None
        while True:
            try:
                resp = stub.GetResult(
                    GetResultRequest(task_id=row["task_id"]), timeout=60
                )
                row["polls"] += 1
            except grpc.RpcError as exc:
                if exc.code() in RETRYABLE:
                    time.sleep(interval)
                    interval = min(cfg["poll_max_interval"], interval * 1.5)
                    continue
                raise

            if resp.status == TaskStatus.TASK_STATUS_RUNNING:
                if started is None:
                    started = time.monotonic()
                    row["started_at"] = utc_now()
            elif resp.status == TaskStatus.TASK_STATUS_SUCCESS:
                success_resp = resp
                t_done = time.monotonic()
                break
            elif resp.status == TaskStatus.TASK_STATUS_FAILED:
                t_done = time.monotonic()
                raise TaskFailed(resp.error or "task reported FAILED")

            # PENDING / RUNNING / UNSPECIFIED: keep waiting.
            time.sleep(interval)
            interval = min(cfg["poll_max_interval"], interval * 1.5)

        assert success_resp is not None
        row["status"] = "success"
        row["chunk_count"] = len(success_resp.chunks)
        if success_resp.HasField("result"):
            meta = success_resp.result.doc_meta
            row["total_pages"] = meta.total_pages
            row["title"] = meta.title
            row["warning"] = meta.warning

        payload: dict[str, Any] = {
            "run_id": cfg["run_id"],
            "doc_id": doc_id,
            "path": rel,
            "task_id": row["task_id"],
            "source": source,
            "chunk_count": row["chunk_count"],
            "doc_meta": {
                "total_pages": row["total_pages"],
                "title": row["title"],
                "warning": row["warning"],
            },
            "chunks": [],
        }
        if not cfg["no_chunks"]:
            payload["chunks"] = [
                {
                    "chunk_id": c.chunk_id,
                    "text": c.text,
                    "page": c.metadata.page,
                    "heading": c.metadata.heading,
                    "chunk_type": c.metadata.chunk_type,
                }
                for c in success_resp.chunks
            ]

        result_name = f"{idx:05d}_{_safe_stem(rel)}.json"
        target = results_dir / result_name
        tmp = target.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False)
        tmp.replace(target)
        row["result_file"] = result_name
    except TaskFailed as exc:
        row["status"] = "failed"
        row["error"] = str(exc)
    except grpc.RpcError as exc:
        row["status"] = "failed"
        row["error"] = f"{exc.code().name}: {exc.details()}"
    except Exception as exc:  # noqa: BLE001 - record, don't kill the batch
        row["status"] = "failed"
        row["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        channel.close()
        if t_done is None:
            t_done = time.monotonic()
        row["finished_at"] = utc_now()
        if t_submitted is None:
            row["total_s"] = round(t_done - t0, 3)
        else:
            row["total_s"] = round(t_done - t_submitted, 3)
            if started is not None:
                row["queue_s"] = round(started - t_submitted, 3)
                row["exec_s"] = round(t_done - started, 3)
        # A transport error before SubmitParse completed leaves started unset.
        if started is None and row["status"] == "success":
            row["queue_s"] = None
            row["exec_s"] = None

    return row


# --------------------------------------------------------------------------- #
# enumeration / resume
# --------------------------------------------------------------------------- #
def enumerate_files(
    root: Path,
    exts: set[str],
    excludes: list[str],
    include_empty: bool,
) -> tuple[list[Path], dict[str, int]]:
    files: list[Path] = []
    skipped = {"hidden": 0, "system": 0, "ext": 0, "excluded": 0, "empty": 0, "error": 0}

    def on_error(exc: OSError) -> None:
        LOG.warning("directory scan error: %s", exc)

    for dirpath, dirnames, filenames in os.walk(root, onerror=on_error):
        dirnames[:] = sorted(d for d in dirnames if not d.startswith("."))
        for name in sorted(filenames):
            if name.startswith(".") or name.lower() in SYSTEM_FILES:
                skipped["hidden" if name.startswith(".") else "system"] += 1
                continue
            path = Path(dirpath) / name
            rel = path.relative_to(root).as_posix()
            if exts and path.suffix.lower() not in exts:
                skipped["ext"] += 1
                continue
            if any(fnmatch(rel, pattern) for pattern in excludes):
                skipped["excluded"] += 1
                continue
            try:
                size = path.stat().st_size
            except OSError as exc:
                LOG.warning("skip unreadable %s: %s", rel, exc)
                skipped["error"] += 1
                continue
            if size == 0 and not include_empty:
                skipped["empty"] += 1
                continue
            files.append(path)

    files.sort(key=lambda p: p.relative_to(root).as_posix())
    return files, skipped


def load_successful_paths(out_dir: Path) -> set[str]:
    """Relative paths recorded as successful in an earlier run's files.jsonl."""
    path = out_dir / "files.jsonl"
    done: set[str] = set()
    if not path.exists():
        return done
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if row.get("status") == "success" and row.get("path"):
                done.add(row["path"])
    return done


# --------------------------------------------------------------------------- #
# summary
# --------------------------------------------------------------------------- #
def _write_summary(
    out_dir: Path,
    run_id: str,
    started_at: str,
    args: argparse.Namespace,
    files: list[Path],
    skipped: dict[str, int],
    skipped_resume: int,
    rows: list[dict[str, Any]],
    wall_s: float,
    interrupted: bool,
    containers: list[str],
    container_root: str,
    staged: bool,
    staged_bytes: int,
) -> dict[str, Any]:
    success = sum(1 for r in rows if r["status"] == "success")
    failed = sum(1 for r in rows if r["status"] == "failed")
    skipped_existing = sum(1 for r in rows if r["status"] == "skipped")
    submitted = sum(1 for r in rows if r.get("action") == "submitted")
    requeued = sum(1 for r in rows if r.get("action") == "requeued")
    attached = sum(1 for r in rows if r.get("action") == "attached")
    queue_values = [r["queue_s"] for r in rows if r["queue_s"] is not None]
    exec_values = [r["exec_s"] for r in rows if r["exec_s"] is not None]
    total_values = [r["total_s"] for r in rows if r["total_s"] is not None]
    submit_values = [r["submit_s"] for r in rows if r["submit_s"] is not None]
    completed = success + failed + skipped_existing

    summary = {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": utc_now(),
        "interrupted": interrupted,
        "config": {
            "root": str(Path(args.root).expanduser().resolve()),
            "addr": args.addr,
            "ext": args.ext,
            "exclude": args.exclude,
            "limit": args.limit,
            "no_chunks": args.no_chunks,
            "poll_interval_s": args.poll_interval,
            "poll_max_interval_s": args.poll_max_interval,
            "workers": len(files),
            "stage": args.stage,
            "container_root": container_root,
            "project": args.project,
            "service": args.service,
        },
        "files": {
            "selected": len(files),
            "skipped_resume": skipped_resume,
            "skipped_by_reason": skipped,
        },
        "outcome": {
            "submitted": submitted,
            "requeued": requeued,
            "attached": attached,
            "skipped_existing": skipped_existing,
            "success": success,
            "failed": failed,
            "not_finished": max(0, len(files) - completed),
        },
        "timings_s": {
            "submit": _summary_stats(submit_values),
            "queue": _summary_stats(queue_values),
            "exec": _summary_stats(exec_values),
            "total": _summary_stats(total_values),
            "started_unobserved": sum(1 for r in rows if r["started_at"] is None),
            "wall": round(wall_s, 3),
        },
        "throughput": {
            "completed": completed,
            "wall_s": round(wall_s, 3),
            "files_per_s": round(completed / wall_s, 4) if wall_s > 0 else None,
            "files_per_min": round(completed / wall_s * 60.0, 3) if wall_s > 0 else None,
        },
        "staging": {
            "mode": (
                "staged"
                if staged
                else ("pre-mounted" if args.stage == "auto" and containers else "none")
            ),
            "enabled": staged,
            "workers": [c[:12] for c in containers],
            "container_root": container_root,
            "staged_bytes": staged_bytes,
        },
    }
    target = out_dir / "summary.json"
    tmp = target.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    tmp.replace(target)
    return summary


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--root", default="D:/MMLongBench-Doc/documents", help="directory to parse")
    ap.add_argument("--addr", default="localhost:50051", help="ParseService gRPC address")
    ap.add_argument(
        "--out-dir",
        default=None,
        help="persist results/logs here (default: out/benchmark-<run-id>; "
        "required with --resume)",
    )
    ap.add_argument("--ext", action="append", default=None, metavar=".pdf",
                    help="only these extensions (repeatable, e.g. --ext .pdf --ext .png)")
    ap.add_argument("--exclude", action="append", default=[], metavar="GLOB",
                    help="skip paths matching this glob (repeatable)")
    ap.add_argument("--limit", type=int, default=0, help="stop after N selected files")
    ap.add_argument("--resume", action="store_true",
                    help="skip files already recorded as successful in --out-dir")
    ap.add_argument("--include-empty", action="store_true", help="do not skip 0-byte files")
    ap.add_argument("--poll-interval", type=float, default=0.5,
                    help="initial GetResult interval per task (seconds)")
    ap.add_argument("--poll-max-interval", type=float, default=5.0,
                    help="maximum GetResult backoff (seconds)")
    ap.add_argument("--stage", choices=("auto", "none"), default="auto",
                    help="auto: tar the selected files into every worker container; "
                    "none: assume they are already readable in the containers")
    ap.add_argument("--container-root", default=None,
                    help="path inside the workers that holds the files "
                    "(default: /tmp/py_parse_bench/<run-id>)")
    ap.add_argument("--project", default="py-parse", help="compose project label")
    ap.add_argument("--service", default="celery-worker", help="compose service label")
    ap.add_argument("--keep-staged", action="store_true",
                    help="do not delete staged files from the containers when done")
    ap.add_argument("--no-chunks", action="store_true",
                    help="persist only metadata, not chunk text")
    ap.add_argument("--dry-run", action="store_true",
                    help="enumerate and show the staging plan, then exit")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    started_at = utc_now()

    root = Path(args.root).expanduser().resolve()
    if not root.is_dir():
        print(f"error: --root is not a directory: {root}", file=sys.stderr)
        return 2
    if args.resume and not args.out_dir:
        print("error: --resume requires --out-dir of an earlier run", file=sys.stderr)
        return 2

    out_dir = (
        Path(args.out_dir).expanduser()
        if args.out_dir
        else Path("out") / f"benchmark-{run_id}"
    )
    (out_dir / "results").mkdir(parents=True, exist_ok=True)
    setup_logging(out_dir / "run.log")

    exts = {
        (e.lower() if e.startswith(".") else "." + e.lower())
        for e in (args.ext or [])
    }
    files, skipped = enumerate_files(root, exts, args.exclude, args.include_empty)

    skipped_resume = 0
    if args.resume:
        done = load_successful_paths(out_dir)
        before = len(files)
        files = [p for p in files if p.relative_to(root).as_posix() not in done]
        skipped_resume = before - len(files)
        LOG.info("resume: %d file(s) already successful, %d left", skipped_resume, len(files))

    if args.limit > 0:
        files = files[: args.limit]

    staged_bytes = sum(_safe_size(p) or 0 for p in files)
    LOG.info(
        "run %s: root=%s selected=%d (%.1f MiB) skipped=%s",
        run_id,
        root,
        len(files),
        staged_bytes / 2**20,
        skipped,
    )
    if not files:
        LOG.warning("nothing to do")
        return 0

    container_root = args.container_root or f"/tmp/py_parse_bench/{run_id}"
    container_root = container_root.rstrip("/") or "/"
    containers: list[str] = []
    staged = False

    if args.dry_run:
        if args.stage == "auto":
            containers = discover_workers(args.project, args.service)
            mounted = detect_mounted_root(containers, root) if containers else None
            if mounted:
                LOG.info(
                    "dry run: %d worker(s) already mount %s at %s; staging would be skipped",
                    len(containers),
                    root,
                    mounted,
                )
            else:
                LOG.info("dry run: would stage into %d worker(s): %s",
                         len(containers), ", ".join(c[:12] for c in containers) or "-")
        LOG.info(
            "dry run: would submit %d file(s) at once, one worker per file, "
            "sources file://%s/<rel>",
            len(files),
            container_root,
        )
        return 0

    # Fail fast if the control plane is unreachable.
    channel = grpc.insecure_channel(
        args.addr,
        options=[("grpc.max_receive_message_length", GRPC_MAX_MESSAGE_BYTES)],
    )
    try:
        grpc.channel_ready_future(channel).result(timeout=10)
    except grpc.FutureTimeoutError:
        LOG.error("cannot reach ParseService at %s", args.addr)
        channel.close()
        return 2
    channel.close()

    if args.stage == "auto":
        containers = discover_workers(args.project, args.service)
        if not containers:
            LOG.error(
                "no running '%s' containers for compose project '%s'. "
                "Start the stack, or pass --stage none if the files are already "
                "readable inside the workers.",
                args.service,
                args.project,
            )
            return 2
        mounted = detect_mounted_root(containers, root)
        if mounted is not None:
            if args.container_root and args.container_root.rstrip("/") != mounted.rstrip("/"):
                LOG.warning(
                    "--container-root=%s ignored: the workers already mount the "
                    "directory at %s",
                    args.container_root,
                    mounted,
                )
            container_root = mounted
            LOG.info(
                "workers already bind-mount %s at %s; staging skipped",
                root,
                container_root,
            )
        else:
            if args.container_root:
                LOG.warning(
                    "--container-root was given; staged files will be written there "
                    "and will NOT be deleted afterwards"
                )
            try:
                stage_into_workers(containers, files, root, container_root, staged_bytes)
            except KeyboardInterrupt:
                LOG.warning("interrupted during staging; removing partial files")
                cleanup_staged(containers, container_root)
                return 130
            except RuntimeError as exc:
                LOG.error("staging failed: %s", exc)
                return 2
            staged = True
    else:
        LOG.info(
            "staging skipped; sources are read as file://%s/<rel> inside the workers",
            container_root,
        )

    # One lookup for the whole selection: the per-source decision (submit /
    # skip / requeue / attach) must not be made file by file, or every file
    # would pay an extra round trip.
    sources = [
        _source_uri(p.relative_to(root).as_posix(), container_root) for p in files
    ]
    try:
        existing = lookup_existing(args.addr, sources)
    except grpc.RpcError as exc:
        LOG.error("ListTasks failed: %s: %s", exc.code().name, exc.details())
        return 2
    counts = {"success": 0, "failed": 0, "inflight": 0}
    for source in sources:
        task = existing.get(source)
        if task is None:
            continue
        if task.status == TaskStatus.TASK_STATUS_SUCCESS:
            counts["success"] += 1
        elif task.status == TaskStatus.TASK_STATUS_FAILED:
            counts["failed"] += 1
        else:
            counts["inflight"] += 1
    LOG.info(
        "lookup: %d source(s) — %d success (skip), %d failed (requeue), "
        "%d in flight (attach), %d new (submit)",
        len(sources),
        counts["success"],
        counts["failed"],
        counts["inflight"],
        len(sources) - len(existing),
    )

    cfg: dict[str, Any] = {
        "run_id": run_id,
        "addr": args.addr,
        "root": str(root),
        "container_root": container_root,
        "results_dir": str(out_dir / "results"),
        "no_chunks": args.no_chunks,
        "poll_interval": args.poll_interval,
        "poll_max_interval": args.poll_max_interval,
        "existing": existing,
    }

    LOG.info(
        "processing %d file(s) with %d worker(s); timings -> %s",
        len(files),
        len(files),
        out_dir / "files.jsonl",
    )

    rows: list[dict[str, Any]] = []
    interrupted = False
    wall_started = time.monotonic()
    executor = ThreadPoolExecutor(
        max_workers=len(files), thread_name_prefix="parse-file"
    )
    futures = {
        executor.submit(_run_file, idx, str(path), cfg): (idx, path)
        for idx, path in enumerate(files)
    }
    try:
        for finished, fut in enumerate(as_completed(futures), 1):
            idx, path = futures[fut]
            try:
                row = fut.result()
            except Exception as exc:  # noqa: BLE001 - a dead worker must not kill the run
                row = {
                    "run_id": run_id,
                    "index": idx,
                    "doc_id": _make_doc_id(path.relative_to(root).as_posix()),
                    "path": path.relative_to(root).as_posix(),
                    "task_id": "",
                    "source": "",
                    "action": "",
                    "status": "failed",
                    "error": f"worker failed: {type(exc).__name__}: {exc}",
                    "submitted_at": None,
                    "started_at": None,
                    "finished_at": utc_now(),
                    "submit_s": None,
                    "queue_s": None,
                    "exec_s": None,
                    "total_s": None,
                    "polls": 0,
                    "size_bytes": _safe_size(path),
                    "chunk_count": 0,
                    "total_pages": 0,
                    "title": "",
                    "warning": "",
                    "result_file": None,
                }
            rows.append(row)
            append_jsonl(out_dir / "files.jsonl", row)
            LOG.info(
                "[%d/%d] %-7s %-9s queue=%ss exec=%ss total=%ss chunks=%d  %s%s",
                finished,
                len(files),
                row["status"].upper(),
                row.get("action", ""),
                _fmt(row["queue_s"]),
                _fmt(row["exec_s"]),
                _fmt(row["total_s"]),
                row["chunk_count"],
                row["path"],
                "" if row["status"] in ("success", "skipped")
                else f"  ({row['error'][:200]})",
            )
    except KeyboardInterrupt:
        interrupted = True
        LOG.warning("interrupted; queued pollers cancelled, running pollers may continue")
        executor.shutdown(wait=False, cancel_futures=True)
    else:
        executor.shutdown(wait=True)

    wall_s = time.monotonic() - wall_started
    summary = _write_summary(
        out_dir,
        run_id,
        started_at,
        args,
        files,
        skipped,
        skipped_resume,
        rows,
        wall_s,
        interrupted,
        containers,
        container_root,
        staged,
        staged_bytes if staged else 0,
    )

    # Only delete a directory this script created for this run; a
    # user-provided --container-root is left alone.
    if (
        staged
        and containers
        and not args.container_root
        and not args.keep_staged
        and not interrupted
    ):
        cleanup_staged(containers, container_root)

    outcome = summary["outcome"]
    LOG.info(
        "done: success=%d skipped=%d failed=%d not_finished=%d in %.1fs; "
        "throughput=%.2f files/min",
        outcome["success"],
        outcome["skipped_existing"],
        outcome["failed"],
        outcome["not_finished"],
        summary["throughput"]["wall_s"],
        summary["throughput"]["files_per_min"] or 0.0,
    )
    LOG.info("results: %s", out_dir.resolve())
    if interrupted:
        return 130
    return 0 if outcome["success"] + outcome["skipped_existing"] == len(files) else 1


if __name__ == "__main__":
    sys.exit(main())
