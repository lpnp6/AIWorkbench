#!/usr/bin/env python
"""Batch-parse a local directory through the ParseService gRPC API.

One `SubmitParse` per file, poll `GetResult` until terminal, persist every
result, and continuously log per-file timing plus host CPU/GPU usage and
aggregate throughput.

Why staging?  The Celery workers read `file://` sources from *their own*
container filesystem, and this script must not touch docker/compose.yaml.
So before submitting, it streams the selected files into every running
`celery-worker` container with:

    docker exec -i <worker> tar -xf - -C <container-root>

and submits `file://<container-root>/<relative-path>` URIs.  The worker
containers have no extra mounts; nothing in compose changes.

Output layout (--out-dir):

    results/<index>_<name>.json   full chunk payload per successful file
    files.jsonl                   one row per file: timing, CPU/GPU before/after/peak
    throughput.jsonl              periodic throughput + resource samples
    run.log                       human-readable progress log
    summary.json                  final aggregate (written even on Ctrl-C)

Load mode (--users N) simulates N virtual users. All users go at once: each
one submits its parse (files are handed out round-robin, so the whole corpus
is reused), polls until it gets a result, records what it received, and exits.
The run ends when every user has its result (or after --duration seconds as a
safety limit, 0 = no limit).

Only --max-inflight RPCs are started at the same time (default 512). That is
not a server limit but a client one: grpc.aio cancels calls once a few thousand
are started simultaneously (reproduced: ~7k of 10k concurrent calls come back
CANCELLED), so without the cap the benchmark would measure gRPC's client
queue rather than the service. The time a request spends waiting for this
client-side slot is recorded as queue_s.

Everything a user "received" is recorded per request:

    requests.jsonl   user_id, file, status (success/failed/timeout/aborted),
                     server error text, submit latency, wait latency,
                     chunk count, pages, title, first-chunk preview

Load mode skips full result persistence by default (10k users produce far too
much data); pass --save-results to keep every payload. Note that the parses
themselves keep running server-side after the load phase ends: the gRPC API
has no cancel, so queued Celery tasks are drained by the workers afterwards.
For the same reason, load mode never deletes staged files at the end of the
run (a bind mount, as in compose.yaml, has nothing to delete).

Examples:

    # smoke test: 5 PDFs
    python scripts/benchmark_parse.py --root D:/MMLongBench-Doc/documents --ext .pdf --limit 5

    # everything, resumable
    python scripts/benchmark_parse.py --root D:/MMLongBench-Doc/documents --out-dir out/bench-full --resume

    # files are already present in the workers (no staging)
    python scripts/benchmark_parse.py --root D:/MMLongBench-Doc/documents --stage none \
        --container-root /data/books

    # 10k virtual users, one parse each, run until every result is in
    python scripts/benchmark_parse.py --users 10000 \
        --ext .pdf --ext .epub --out-dir out/load-10k
"""
from __future__ import annotations

import argparse
import asyncio
import ctypes
import hashlib
import json
import logging
import math
import os
import random
import re
import shutil
import subprocess
import sys
import tarfile
import threading
import time
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from fnmatch import fnmatch
from pathlib import Path
from typing import Any
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import grpc  # noqa: E402

from py_parse.generated import (  # noqa: E402
    GetResultRequest,
    ParseRequest,
    ParseServiceStub,
    TaskStatus,
)

LOG = logging.getLogger("bench")

# tasks.doc_id is varchar(256); keep a safe margin.
MAX_DOC_ID_LEN = 240
# Result payloads can be large (many chunks); the server's default client-side
# receive limit is 4 MiB, which is easy to exceed on a book-length document.
GRPC_MAX_MESSAGE_BYTES = 512 * 1024 * 1024
RETRYABLE = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}

SYSTEM_FILES = {"thumbs.db", "desktop.ini", ".ds_store"}
_BAD_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_JSONL_LOCK = threading.Lock()
# Path -> open append handle. Load mode can emit thousands of rows per second;
# opening the file per row would dominate the benchmark itself.
_JSONL_HANDLES: dict[str, Any] = {}


# --------------------------------------------------------------------------- #
# small helpers
# --------------------------------------------------------------------------- #
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    line = json.dumps(row, ensure_ascii=False, default=str)
    key = str(path)
    with _JSONL_LOCK:
        fh = _JSONL_HANDLES.get(key)
        if fh is None:
            fh = path.open("a", encoding="utf-8")
            _JSONL_HANDLES[key] = fh
        fh.write(line + "\n")
        fh.flush()


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


def _make_doc_id(rel: str, max_len: int = MAX_DOC_ID_LEN) -> str:
    """Fit a relative path into tasks.doc_id (varchar(256)).

    Short paths are used verbatim; long ones keep a hash prefix plus the tail
    (the filename is what matters when reading server logs).
    """
    if len(rel) <= max_len:
        return rel
    digest = hashlib.sha1(rel.encode("utf-8")).hexdigest()[:10]
    tail = rel[-(max_len - len(digest) - 1):]
    return f"{digest}:{tail}"


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
# resource sampling: host CPU + NVIDIA GPU
# --------------------------------------------------------------------------- #
class _FileTime(ctypes.Structure):
    _fields_ = [("dwLowDateTime", ctypes.c_uint32), ("dwHighDateTime", ctypes.c_uint32)]


def _windows_times() -> tuple[int, int] | None:
    """(idle, kernel+user) 100ns ticks, system-wide."""
    idle, kernel, user = _FileTime(), _FileTime(), _FileTime()
    ok = ctypes.windll.kernel32.GetSystemTimes(  # type: ignore[attr-defined]
        ctypes.byref(idle), ctypes.byref(kernel), ctypes.byref(user)
    )
    if not ok:
        return None

    def conv(ft: _FileTime) -> int:
        return (ft.dwHighDateTime << 32) | ft.dwLowDateTime

    # Kernel time already includes idle time.
    return conv(idle), conv(kernel) + conv(user)


def _linux_times() -> tuple[int, int] | None:
    try:
        with open("/proc/stat", "r", encoding="ascii") as fh:
            parts = fh.readline().split()
    except OSError:
        return None
    if not parts or parts[0] != "cpu":
        return None
    values = [int(v) for v in parts[1:]]
    idle = values[3] + (values[4] if len(values) > 4 else 0)  # idle + iowait
    return idle, sum(values)


class CpuMeter:
    """System-wide CPU utilisation between consecutive samples."""

    def __init__(self) -> None:
        self._read = _windows_times if os.name == "nt" else _linux_times
        self._last = self._read()

    def percent(self) -> float | None:
        cur = self._read()
        if cur is None:
            return None
        if self._last is None:
            self._last = cur
            return None
        (idle0, total0), (idle1, total1) = self._last, cur
        self._last = cur
        if total1 <= total0:
            return None
        pct = 100.0 * (1.0 - (idle1 - idle0) / (total1 - total0))
        return max(0.0, min(100.0, pct))


def read_gpu() -> tuple[float | None, float | None, float | None]:
    """(mean util %, total memory used MiB, total memory MiB) across GPUs."""
    exe = shutil.which("nvidia-smi")
    if not exe:
        return None, None, None
    try:
        out = subprocess.run(
            [
                exe,
                "--query-gpu=utilization.gpu,memory.used,memory.total",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
    except (OSError, subprocess.TimeoutExpired):
        return None, None, None

    utils: list[float] = []
    used: list[float] = []
    total: list[float] = []
    for line in out.strip().splitlines():
        cols = [c.strip() for c in line.split(",")]
        if len(cols) != 3:
            continue
        try:
            utils.append(float(cols[0]))
            used.append(float(cols[1]))
            total.append(float(cols[2]))
        except ValueError:
            continue  # e.g. [N/A] on some WSL/driver combinations
    if not utils:
        return None, None, None
    return sum(utils) / len(utils), sum(used), sum(total)


@dataclass(frozen=True)
class Sample:
    t: float
    cpu_pct: float | None
    gpu_util_pct: float | None
    gpu_mem_used_mib: float | None
    gpu_mem_total_mib: float | None


class ResourceSampler(threading.Thread):
    def __init__(self, interval: float) -> None:
        super().__init__(name="resource-sampler", daemon=True)
        self._interval = interval
        self._samples: deque[Sample] = deque(maxlen=20_000)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._meter = CpuMeter()
        self._peak_cpu = 0.0
        self._peak_gpu_util = 0.0
        self._peak_gpu_mem = 0.0
        self._gpu_total: float | None = None
        self._has_cpu = False
        self._has_gpu = False

    def run(self) -> None:
        while not self._stop.is_set():
            cpu = self._meter.percent()
            gpu_util, gpu_used, gpu_total = read_gpu()
            sample = Sample(time.time(), cpu, gpu_util, gpu_used, gpu_total)
            with self._lock:
                self._samples.append(sample)
                if cpu is not None:
                    self._has_cpu = True
                    self._peak_cpu = max(self._peak_cpu, cpu)
                if gpu_util is not None:
                    self._has_gpu = True
                    self._peak_gpu_util = max(self._peak_gpu_util, gpu_util)
                if gpu_used is not None:
                    self._peak_gpu_mem = max(self._peak_gpu_mem, gpu_used)
                if gpu_total is not None:
                    self._gpu_total = gpu_total
            self._stop.wait(self._interval)

    def stop(self) -> None:
        self._stop.set()

    def latest(self) -> Sample:
        with self._lock:
            if self._samples:
                return self._samples[-1]
        return Sample(time.time(), None, None, None, None)

    def peak_between(self, t0: float, t1: float) -> Sample:
        with self._lock:
            window = [s for s in self._samples if t0 <= s.t <= t1]
        if not window:
            return Sample(t1, None, None, None, None)
        return Sample(
            t1,
            max((s.cpu_pct for s in window if s.cpu_pct is not None), default=None),
            max((s.gpu_util_pct for s in window if s.gpu_util_pct is not None), default=None),
            max(
                (s.gpu_mem_used_mib for s in window if s.gpu_mem_used_mib is not None),
                default=None,
            ),
            next(
                (
                    s.gpu_mem_total_mib
                    for s in reversed(window)
                    if s.gpu_mem_total_mib is not None
                ),
                None,
            ),
        )

    def overall_peaks(self) -> dict[str, float | None]:
        with self._lock:
            return {
                "peak_cpu_pct": round(self._peak_cpu, 1) if self._has_cpu else None,
                "peak_gpu_util_pct": round(self._peak_gpu_util, 1) if self._has_gpu else None,
                "peak_gpu_mem_used_mib": (
                    round(self._peak_gpu_mem, 1) if self._peak_gpu_mem else None
                ),
                "gpu_mem_total_mib": self._gpu_total,
            }


# --------------------------------------------------------------------------- #
# counters + throughput reporter
# --------------------------------------------------------------------------- #
class Stats:
    def __init__(self) -> None:
        self.start = time.monotonic()
        self._lock = threading.Lock()
        self.submitted = 0
        self.finished = 0
        self.succeeded = 0
        self.failed = 0
        self.timed_out = 0
        self.aborted = 0
        self.chunks = 0
        self.pages = 0
        # Bounded: load mode can finish millions of requests in a long run.
        self.durations: deque[float] = deque(maxlen=100_000)
        self._done_at: deque[float] = deque(maxlen=100_000)

    def mark_submitted(self) -> None:
        with self._lock:
            self.submitted += 1

    def mark_finished(
        self,
        ok: bool,
        duration: float,
        *,
        chunks: int = 0,
        pages: int = 0,
        timed_out: bool = False,
        aborted: bool = False,
    ) -> None:
        with self._lock:
            self.finished += 1
            if ok:
                self.succeeded += 1
            elif aborted:
                self.aborted += 1
            elif timed_out:
                self.timed_out += 1
            else:
                self.failed += 1
            self.chunks += chunks
            self.pages += pages
            self.durations.append(duration)
            self._done_at.append(time.monotonic())

    def snapshot(self, window: float) -> dict[str, Any]:
        now = time.monotonic()
        with self._lock:
            while self._done_at and now - self._done_at[0] > window:
                self._done_at.popleft()
            recent = len(self._done_at)
            elapsed = max(now - self.start, 1e-9)
            span = min(window, elapsed)
            durations = self.durations
            return {
                "elapsed_s": elapsed,
                "submitted": self.submitted,
                "finished": self.finished,
                "in_flight": self.submitted - self.finished,
                "success": self.succeeded,
                "failed": self.failed,
                "timed_out": self.timed_out,
                "aborted": self.aborted,
                "chunks": self.chunks,
                "pages": self.pages,
                "files_per_min_total": self.finished / elapsed * 60.0,
                "files_per_min_recent": (recent / span * 60.0) if span > 0 else 0.0,
                "recent_window_s": span,
                "avg_duration_s": (sum(durations) / len(durations)) if durations else 0.0,
            }


class ThroughputReporter(threading.Thread):
    def __init__(
        self,
        run_id: str,
        stats: Stats,
        sampler: ResourceSampler,
        path: Path,
        interval: float,
        window: float,
        *,
        label: str = "files",
        queue_depth_fn: Any = None,
    ) -> None:
        super().__init__(name="throughput-reporter", daemon=True)
        self._run_id = run_id
        self._stats = stats
        self._sampler = sampler
        self._path = path
        self._interval = interval
        self._window = window
        self._label = label
        self._queue_depth_fn = queue_depth_fn
        self._stop = threading.Event()

    def run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                self.report_once()
            except Exception:  # noqa: BLE001 - a reporting failure must not kill the run
                LOG.exception("throughput report failed")

    def stop(self) -> None:
        self._stop.set()

    def report_once(self) -> None:
        snap = self._stats.snapshot(self._window)
        s = self._sampler.latest()
        queue_depth = None
        if self._queue_depth_fn is not None:
            try:
                queue_depth = self._queue_depth_fn()
            except Exception:  # noqa: BLE001 - reporting must never break the run
                queue_depth = None
        row = {
            "run_id": self._run_id,
            "ts": utc_now(),
            "elapsed_s": round(snap["elapsed_s"], 1),
            "submitted": snap["submitted"],
            "finished": snap["finished"],
            "in_flight": snap["in_flight"],
            "success": snap["success"],
            "failed": snap["failed"],
            "timeout": snap["timed_out"],
            "aborted": snap["aborted"],
            "files_per_min_total": round(snap["files_per_min_total"], 2),
            "files_per_min_recent": round(snap["files_per_min_recent"], 2),
            "avg_duration_s": round(snap["avg_duration_s"], 2),
            "chunks": snap["chunks"],
            "pages": snap["pages"],
            "cpu_pct": _round(s.cpu_pct),
            "gpu_util_pct": _round(s.gpu_util_pct),
            "gpu_mem_used_mib": _round(s.gpu_mem_used_mib),
            "gpu_mem_total_mib": _round(s.gpu_mem_total_mib),
            "queue_depth": queue_depth,
        }
        append_jsonl(self._path, row)
        LOG.info(
            "throughput(%s): finished=%d/%d ok=%d fail=%d abort=%d inflight=%d | "
            "total=%.2f/min recent=%.2f/min avg=%.1fs | queue=%s cpu=%s%% gpu=%s%% mem=%s MiB",
            self._label,
            snap["finished"],
            snap["submitted"],
            snap["success"],
            snap["failed"],
            snap["aborted"],
            snap["in_flight"],
            snap["files_per_min_total"],
            snap["files_per_min_recent"],
            snap["avg_duration_s"],
            "-" if queue_depth is None else queue_depth,
            _fmt(s.cpu_pct),
            _fmt(s.gpu_util_pct),
            _fmt(s.gpu_mem_used_mib, 0),
        )


# --------------------------------------------------------------------------- #
# per-file submit → poll → persist
# --------------------------------------------------------------------------- #
class TaskFailed(Exception):
    pass


class Runner:
    def __init__(
        self,
        args: argparse.Namespace,
        run_id: str,
        stub: ParseServiceStub,
        stats: Stats,
        sampler: ResourceSampler,
        out_dir: Path,
        container_root: str,
    ) -> None:
        self.args = args
        self.run_id = run_id
        self.stub = stub
        self.stats = stats
        self.sampler = sampler
        self.root = Path(args.root).expanduser().resolve()
        self.results_dir = out_dir / "results"
        self.container_root = container_root.rstrip("/")
        self.files_jsonl = out_dir / "files.jsonl"

    # -- gRPC ------------------------------------------------------------- #
    def _submit(self, doc_id: str, source: str) -> str:
        delay = 1.0
        for attempt in range(5):
            try:
                resp = self.stub.SubmitParse(
                    ParseRequest(doc_id=doc_id, source=source), timeout=30
                )
                return resp.task_id
            except grpc.RpcError as exc:
                if exc.code() in RETRYABLE and attempt < 4:
                    LOG.warning(
                        "SubmitParse retry %d for %s: %s",
                        attempt + 1,
                        doc_id,
                        exc.details(),
                    )
                    time.sleep(delay)
                    delay *= 2
                    continue
                raise
        raise RuntimeError("unreachable")

    def _poll(self, task_id: str):
        deadline = time.monotonic() + self.args.task_timeout
        interval = self.args.poll_interval
        while True:
            try:
                resp = self.stub.GetResult(GetResultRequest(task_id=task_id), timeout=60)
            except grpc.RpcError as exc:
                if exc.code() in RETRYABLE and time.monotonic() < deadline:
                    time.sleep(interval)
                    interval = min(self.args.poll_max_interval, interval * 1.5)
                    continue
                raise

            if resp.status == TaskStatus.TASK_STATUS_SUCCESS:
                return resp
            if resp.status == TaskStatus.TASK_STATUS_FAILED:
                raise TaskFailed(resp.error or "task reported FAILED")
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"task {task_id} not finished within {self.args.task_timeout:.0f}s"
                )
            time.sleep(interval)
            interval = min(self.args.poll_max_interval, interval * 1.5)

    # -- persistence ------------------------------------------------------ #
    def _result_name(self, idx: int, rel: str) -> str:
        return f"{idx:05d}_{_safe_stem(rel)}.json"

    def _write_result(self, idx: int, rel: str, doc_id: str, task_id: str, source: str, resp) -> None:
        payload: dict[str, Any] = {
            "run_id": self.run_id,
            "doc_id": doc_id,
            "path": rel,
            "task_id": task_id,
            "source": source,
            "chunk_count": len(resp.chunks),
            "doc_meta": {},
            "chunks": [],
        }
        if resp.HasField("result"):
            meta = resp.result.doc_meta
            payload["doc_meta"] = {
                "total_pages": meta.total_pages,
                "title": meta.title,
                "warning": meta.warning,
            }
        if not self.args.no_chunks:
            payload["chunks"] = [
                {
                    "chunk_id": c.chunk_id,
                    "text": c.text,
                    "page": c.metadata.page,
                    "heading": c.metadata.heading,
                    "chunk_type": c.metadata.chunk_type,
                }
                for c in resp.chunks
            ]
            payload["chunk_count"] = len(payload["chunks"])

        target = self.results_dir / self._result_name(idx, rel)
        tmp = target.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False)
        tmp.replace(target)

    # -- one file --------------------------------------------------------- #
    def run_one(self, idx: int, total: int, path: Path) -> None:
        rel = path.relative_to(self.root).as_posix()
        doc_id = _make_doc_id(rel)
        source = "file://" + quote(f"{self.container_root}/{rel}", safe="/")
        started_at = utc_now()

        t0 = time.time()
        before = self.sampler.latest()
        self.stats.mark_submitted()

        status = "failed"
        error = ""
        task_id = ""
        chunk_count = 0
        pages = 0
        title = ""
        warning = ""
        timed_out = False

        try:
            task_id = self._submit(doc_id, source)
            resp = self._poll(task_id)
            status = "success"
            chunk_count = len(resp.chunks)
            if resp.HasField("result"):
                pages = resp.result.doc_meta.total_pages
                title = resp.result.doc_meta.title
                warning = resp.result.doc_meta.warning
            self._write_result(idx, rel, doc_id, task_id, source, resp)
        except TaskFailed as exc:
            status = "failed"
            error = str(exc)
        except TimeoutError as exc:
            status = "timeout"
            timed_out = True
            error = str(exc)
        except grpc.RpcError as exc:
            status = "failed"
            error = f"{exc.code().name}: {exc.details()}"
        except Exception as exc:  # noqa: BLE001 - record, don't kill the batch
            status = "failed"
            error = f"{type(exc).__name__}: {exc}"
        finally:
            t1 = time.time()
            duration = t1 - t0
            after = self.sampler.latest()
            peak = self.sampler.peak_between(t0, t1)
            ok = status == "success"
            self.stats.mark_finished(
                ok, duration, chunks=chunk_count, pages=pages, timed_out=timed_out
            )

            append_jsonl(
                self.files_jsonl,
                {
                    "run_id": self.run_id,
                    "doc_id": doc_id,
                    "path": rel,
                    "task_id": task_id,
                    "source": source,
                    "status": status,
                    "error": error[:4000],
                    "started_at": started_at,
                    "finished_at": utc_now(),
                    "duration_s": round(duration, 3),
                    "size_bytes": _safe_size(path),
                    "chunk_count": chunk_count,
                    "total_pages": pages,
                    "title": title,
                    "warning": warning,
                    "result_file": self._result_name(idx, rel) if ok else None,
                    "cpu_before_pct": _round(before.cpu_pct),
                    "cpu_after_pct": _round(after.cpu_pct),
                    "cpu_peak_pct": _round(peak.cpu_pct),
                    "gpu_util_before_pct": _round(before.gpu_util_pct),
                    "gpu_util_after_pct": _round(after.gpu_util_pct),
                    "gpu_util_peak_pct": _round(peak.gpu_util_pct),
                    "gpu_mem_before_mib": _round(before.gpu_mem_used_mib),
                    "gpu_mem_after_mib": _round(after.gpu_mem_used_mib),
                    "gpu_mem_peak_mib": _round(peak.gpu_mem_used_mib),
                },
            )

            LOG.info(
                "[%d/%d] %-7s %8.1fs chunks=%-5d cpu %s->%s%% gpu %s->%s%% "
                "mem %s->%s MiB  %s%s",
                idx + 1,
                total,
                status.upper(),
                duration,
                chunk_count,
                _fmt(before.cpu_pct),
                _fmt(after.cpu_pct),
                _fmt(before.gpu_util_pct),
                _fmt(after.gpu_util_pct),
                _fmt(before.gpu_mem_used_mib, 0),
                _fmt(after.gpu_mem_used_mib, 0),
                rel,
                "" if ok else f"  ({error[:200]})",
            )


# --------------------------------------------------------------------------- #
# load mode: N virtual users reusing the same files forever
# --------------------------------------------------------------------------- #
class LoadAborted(Exception):
    """The load phase ended while this user was still waiting."""


def make_queue_depth_probe(project: str, service: str) -> Any:
    """Return a callable reporting the Celery queue length, or None.

    Uses `redis-cli LLEN celery` inside the compose Redis container. The probe
    is best-effort: batch mode passes None, load mode tolerates failures.
    """
    try:
        containers = discover_workers(project, service)
    except RuntimeError:
        return None
    if not containers:
        return None
    cid = containers[0]

    def probe() -> int | None:
        try:
            out = _docker("exec", cid, "redis-cli", "llen", "celery", timeout=10)
            return int(out.strip())
        except (RuntimeError, ValueError):
            return None

    return probe


class LoadRunner:
    """N virtual users, each looping submit -> poll -> record over shared files.

    Files are handed out round-robin from a shared cursor, so every file is
    reused and the initial burst spreads across the whole corpus instead of
    piling onto the alphabetically first ones.
    """

    def __init__(
        self,
        args: argparse.Namespace,
        run_id: str,
        files: list[Path],
        stats: Stats,
        sampler: ResourceSampler,
        out_dir: Path,
        container_root: str,
    ) -> None:
        self.args = args
        self.run_id = run_id
        self.files = list(files)
        if args.shuffle:
            random.shuffle(self.files)
        self.stats = stats
        self.sampler = sampler
        self.root = Path(args.root).expanduser().resolve()
        self.results_dir = out_dir / "results"
        self.requests_path = out_dir / "requests.jsonl"
        self.container_root = container_root.rstrip("/")

        self.channel_count = args.channels or max(
            4, min(256, math.ceil(args.users / 100))
        )
        # At 10k users a 0.5s poll interval means 20k GetResult/s; scale the
        # floor with the user count so the poller itself is not the bottleneck.
        self.poll_start = min(30.0, max(args.poll_interval, args.users / 2000.0))
        self.poll_max = max(args.poll_max_interval, min(120.0, self.poll_start * 3))

        self._cursor = 0
        self._load_deadline: float | None = None
        self._channels: list[Any] = []
        self._stubs: list[ParseServiceStub] = []
        self._sem: asyncio.Semaphore | None = None
        self._submit_latencies: deque[float] = deque(maxlen=200_000)
        self._wait_latencies: deque[float] = deque(maxlen=200_000)
        self._queue_latencies: deque[float] = deque(maxlen=200_000)
        self._errors: Counter[str] = Counter()

    # -- work distribution ------------------------------------------------ #
    def _next_file(self) -> Path:
        path = self.files[self._cursor % len(self.files)]
        self._cursor += 1
        return path

    # -- gRPC ------------------------------------------------------------- #
    async def _submit(
        self,
        stub: ParseServiceStub,
        doc_id: str,
        source: str,
        qwait: list[float],
    ) -> Any:
        assert self._sem is not None
        delay = 1.0
        for attempt in range(5):
            tq = time.monotonic()
            try:
                async with self._sem:
                    qwait[0] += time.monotonic() - tq
                    return await stub.SubmitParse(
                        ParseRequest(doc_id=doc_id, source=source), timeout=30
                    )
            except grpc.aio.AioRpcError as exc:
                # SubmitParse creates a task per call, so a timeout might hide
                # an already-enqueued task; only retry when it never arrived.
                if exc.code() in SUBMIT_RETRYABLE and attempt < 4:
                    await asyncio.sleep(delay)
                    delay *= 2
                    continue
                raise
        raise RuntimeError("unreachable")

    async def _poll(
        self,
        stub: ParseServiceStub,
        task_id: str,
        deadline: float,
        qwait: list[float],
    ):
        assert self._sem is not None
        interval = self.poll_start
        polls = 0
        while True:
            tq = time.monotonic()
            try:
                async with self._sem:
                    qwait[0] += time.monotonic() - tq
                    resp = await stub.GetResult(
                        GetResultRequest(task_id=task_id), timeout=60
                    )
                    polls += 1
            except grpc.aio.AioRpcError as exc:
                now = time.monotonic()
                if (
                    exc.code() in RETRYABLE
                    and now < deadline
                    and (self._load_deadline is None or now < self._load_deadline)
                ):
                    sleep_s = interval
                    if self._load_deadline is not None:
                        sleep_s = min(sleep_s, max(0.05, self._load_deadline - now))
                    await asyncio.sleep(sleep_s)
                    interval = min(self.poll_max, interval * 1.5)
                    continue
                if self._load_deadline is not None and now >= self._load_deadline:
                    raise LoadAborted("load phase ended while polling") from exc
                raise

            if resp.status == TaskStatus.TASK_STATUS_SUCCESS:
                return resp, polls
            if resp.status == TaskStatus.TASK_STATUS_FAILED:
                raise TaskFailed(resp.error or "task reported FAILED")

            now = time.monotonic()
            if self._load_deadline is not None and now >= self._load_deadline:
                raise LoadAborted("load phase ended while waiting for the result")
            if now >= deadline:
                raise TimeoutError(
                    f"task {task_id} not finished within {self.args.task_timeout:.0f}s"
                )
            sleep_s = interval * (0.8 + random.random() * 0.4)
            if self._load_deadline is not None:
                sleep_s = min(sleep_s, max(0.05, self._load_deadline - now))
            await asyncio.sleep(max(0.05, sleep_s))
            interval = min(self.poll_max, interval * 1.5)

    # -- persistence ------------------------------------------------------ #
    def _save_result(
        self, uid: int, seq: int, rel: str, doc_id: str, task_id: str,
        source: str, resp: Any,
    ) -> None:
        payload: dict[str, Any] = {
            "run_id": self.run_id,
            "user_id": uid,
            "seq": seq,
            "doc_id": doc_id,
            "path": rel,
            "task_id": task_id,
            "source": source,
            "chunk_count": len(resp.chunks),
            "doc_meta": {},
            "chunks": [],
        }
        if resp.HasField("result"):
            meta = resp.result.doc_meta
            payload["doc_meta"] = {
                "total_pages": meta.total_pages,
                "title": meta.title,
                "warning": meta.warning,
            }
        if not self.args.no_chunks:
            payload["chunks"] = [
                {
                    "chunk_id": c.chunk_id,
                    "text": c.text,
                    "page": c.metadata.page,
                    "heading": c.metadata.heading,
                    "chunk_type": c.metadata.chunk_type,
                }
                for c in resp.chunks
            ]
        target = self.results_dir / f"load_{uid}_{seq:05d}_{_safe_stem(rel)}.json"
        tmp = target.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False)
        tmp.replace(target)

    # -- one user request -------------------------------------------------- #
    async def _one(self, stub: ParseServiceStub, uid: int, seq: int) -> None:
        path = self._next_file()
        rel = path.relative_to(self.root).as_posix()
        doc_id = _make_doc_id(f"u{uid}-r{seq}-{rel}")
        source = "file://" + quote(f"{self.container_root}/{rel}", safe="/")
        started_at = utc_now()
        t0 = time.monotonic()
        qwait = [0.0]  # seconds spent waiting for a client-side RPC slot
        self.stats.mark_submitted()

        status = "error"
        error = ""
        task_id = ""
        submit_s: float | None = None
        polls = 0
        chunk_count = 0
        pages = 0
        title = ""
        warning = ""
        preview = ""

        try:
            sub = await self._submit(stub, doc_id, source, qwait)
            task_id = sub.task_id
            submit_s = time.monotonic() - t0
            resp, polls = await self._poll(
                stub, task_id, t0 + self.args.task_timeout, qwait
            )

            status = "success"
            chunk_count = len(resp.chunks)
            if resp.HasField("result"):
                pages = resp.result.doc_meta.total_pages
                title = resp.result.doc_meta.title
                warning = resp.result.doc_meta.warning
            if resp.chunks:
                preview = resp.chunks[0].text[:120]
            if self.args.save_results:
                self._save_result(uid, seq, rel, doc_id, task_id, source, resp)
        except TaskFailed as exc:
            status, error = "failed", str(exc)
        except LoadAborted as exc:
            status, error = "aborted", str(exc)
        except TimeoutError as exc:
            status, error = "timeout", str(exc)
        except asyncio.CancelledError:
            status, error = "aborted", "cancelled (load stopped)"
            raise
        except grpc.aio.AioRpcError as exc:
            status, error = "error", f"{exc.code().name}: {exc.details()}"
        except Exception as exc:  # noqa: BLE001 - one bad request must not stop a user
            status, error = "error", f"{type(exc).__name__}: {exc}"
        finally:
            total_s = time.monotonic() - t0
            wait_s = None if submit_s is None else max(0.0, total_s - submit_s)
            ok = status == "success"
            self.stats.mark_finished(
                ok,
                total_s,
                chunks=chunk_count,
                pages=pages,
                timed_out=status == "timeout",
                aborted=status == "aborted",
            )
            if submit_s is not None:
                self._submit_latencies.append(submit_s)
            if wait_s is not None:
                self._wait_latencies.append(wait_s)
            self._queue_latencies.append(qwait[0])
            if not ok:
                self._errors[f"{status}: {error[:120]}".strip()] += 1
            append_jsonl(
                self.requests_path,
                {
                    "run_id": self.run_id,
                    "user_id": uid,
                    "seq": seq,
                    "path": rel,
                    "doc_id": doc_id,
                    "task_id": task_id,
                    "source": source,
                    "status": status,
                    "error": error[:2000],
                    "started_at": started_at,
                    "finished_at": utc_now(),
                    "queue_s": round(qwait[0], 3),
                    "submit_s": _round(submit_s, 4),
                    "wait_s": _round(wait_s, 3),
                    "total_s": round(total_s, 3),
                    "polls": polls,
                    "chunk_count": chunk_count,
                    "total_pages": pages,
                    "title": title,
                    "warning": warning,
                    "first_chunk": preview,
                    "size_bytes": _safe_size(path),
                },
            )

    # -- loop driver ------------------------------------------------------ #
    async def _user(self, uid: int) -> None:
        stub = self._stubs[uid % len(self._stubs)]
        seq = 0
        while True:
            if self._load_deadline is not None and time.monotonic() >= self._load_deadline:
                return
            if self.args.requests_per_user and seq >= self.args.requests_per_user:
                return
            await self._one(stub, uid, seq)
            seq += 1

    async def _delayed_user(self, uid: int, delay: float) -> None:
        if delay > 0:
            await asyncio.sleep(delay)
        await self._user(uid)

    async def run(self) -> None:
        if self.args.duration > 0:
            self._load_deadline = time.monotonic() + self.args.duration

        options = [("grpc.max_receive_message_length", GRPC_MAX_MESSAGE_BYTES)]
        self._channels = [
            grpc.aio.insecure_channel(self.args.addr, options=options)
            for _ in range(self.channel_count)
        ]
        self._stubs = [ParseServiceStub(ch) for ch in self._channels]
        self._sem = asyncio.Semaphore(self.args.max_inflight)
        LOG.info(
            "load: %d user(s), %d gRPC channel(s), max %d in flight, "
            "poll %.1fs..%.1fs over %d file(s)",
            self.args.users, self.channel_count, self.args.max_inflight,
            self.poll_start, self.poll_max, len(self.files),
        )

        tasks = []
        for uid in range(self.args.users):
            delay = self.args.ramp_up * uid / self.args.users if self.args.ramp_up > 0 else 0.0
            tasks.append(asyncio.create_task(self._delayed_user(uid, delay)))
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            for ch in self._channels:
                try:
                    await ch.close()
                except Exception:  # noqa: BLE001 - best-effort shutdown
                    pass


def _latency_summary(values: deque[float]) -> dict[str, Any]:
    ordered = sorted(values)
    if not ordered:
        return {"count": 0, "mean": None, "p50": None, "p90": None, "p95": None, "max": None}
    return {
        "count": len(ordered),
        "mean": round(sum(ordered) / len(ordered), 4),
        "p50": _round(_percentile(ordered, 0.50), 4),
        "p90": _round(_percentile(ordered, 0.90), 4),
        "p95": _round(_percentile(ordered, 0.95), 4),
        "max": round(ordered[-1], 4),
    }


def _write_load_summary(
    out_dir: Path,
    run_id: str,
    args: argparse.Namespace,
    files: list[Path],
    skipped: dict[str, int],
    stats: Stats,
    sampler: ResourceSampler,
    containers: list[str],
    container_root: str,
    staged: bool,
    staged_bytes: int,
    started_at: str,
    interrupted: bool,
    runner: LoadRunner,
) -> dict[str, Any]:
    snap = stats.snapshot(args.window)
    summary = {
        "run_id": run_id,
        "mode": "load",
        "started_at": started_at,
        "finished_at": utc_now(),
        "interrupted": interrupted,
        "config": {
            "root": str(Path(args.root).expanduser().resolve()),
            "addr": args.addr,
            "ext": args.ext,
            "limit": args.limit,
            "users": args.users,
            "duration_s": args.duration,
            "ramp_up_s": args.ramp_up,
            "channels": runner.channel_count,
            "max_inflight": args.max_inflight,
            "requests_per_user": args.requests_per_user,
            "task_timeout_s": args.task_timeout,
            "poll_start_s": round(runner.poll_start, 3),
            "poll_max_s": round(runner.poll_max, 3),
            "stage": args.stage,
            "container_root": container_root,
        },
        "files": {
            "selected": len(files),
            "skipped_by_reason": skipped,
            "requests_jsonl": str(out_dir / "requests.jsonl"),
        },
        "outcome": {
            "submitted": snap["submitted"],
            "success": snap["success"],
            "failed": snap["failed"],
            "timed_out": snap["timed_out"],
            "aborted": snap["aborted"],
            "not_finished": snap["in_flight"],
        },
        "latency_s": {
            # Time waiting for a client-side RPC slot, before submit/poll.
            "client_queue": _latency_summary(runner._queue_latencies),
            "submit": _latency_summary(runner._submit_latencies),
            "wait": _latency_summary(runner._wait_latencies),
            "total": _latency_summary(stats.durations),
        },
        "work": {"chunks": snap["chunks"], "pages": snap["pages"]},
        "errors": dict(runner._errors.most_common(25)),
        "throughput": {
            "requests_per_min_total": round(snap["files_per_min_total"], 3),
            "requests_per_min_recent": round(snap["files_per_min_recent"], 3),
            "window_s": round(snap["recent_window_s"], 1),
            "elapsed_s": round(snap["elapsed_s"], 1),
        },
        "resources": sampler.overall_peaks(),
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
            # Load mode leaves staged files in the workers for the queued
            # tasks that outlive the load phase.
            "retained": bool(staged and containers),
        },
    }
    target = out_dir / "summary.json"
    tmp = target.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    tmp.replace(target)
    return summary


def run_load(
    args: argparse.Namespace,
    run_id: str,
    started_at: str,
    out_dir: Path,
    files: list[Path],
    skipped: dict[str, int],
    containers: list[str],
    container_root: str,
    staged: bool,
    staged_bytes: int,
) -> int:
    """Drive the virtual-user load phase and persist its logs."""
    stats = Stats()
    sampler = ResourceSampler(args.sample_interval)
    sampler.start()
    reporter = ThroughputReporter(
        run_id,
        stats,
        sampler,
        out_dir / "throughput.jsonl",
        args.throughput_interval,
        args.window,
        label="requests",
        queue_depth_fn=make_queue_depth_probe(args.project, args.redis_service),
    )
    reporter.start()
    runner = LoadRunner(args, run_id, files, stats, sampler, out_dir, container_root)

    LOG.info(
        "load: %d user(s) over %d file(s), %d request(s)/user%s; responses -> %s",
        args.users,
        len(files),
        args.requests_per_user or "unlimited",
        "" if args.ramp_up <= 0 and args.duration <= 0
        else f" (ramp-up {args.ramp_up:.0f}s"
        + (f", safety limit {args.duration:.0f}s)" if args.duration > 0 else ")"),
        runner.requests_path,
    )
    interrupted = False
    try:
        asyncio.run(runner.run())
    except KeyboardInterrupt:
        interrupted = True
        LOG.warning("load interrupted; in-flight requests were aborted")
    finally:
        sampler.stop()
        reporter.stop()
        try:
            reporter.report_once()
        except Exception:  # noqa: BLE001
            LOG.exception("final throughput row failed")
        summary = _write_load_summary(
            out_dir, run_id, args, files, skipped, stats, sampler,
            containers, container_root, staged, staged_bytes,
            started_at, interrupted, runner,
        )
        # Unlike batch mode, tasks aborted at the deadline are still queued and
        # will read their source later, so staged files must stay until the
        # workers drain the backlog.
        if staged and containers:
            LOG.warning(
                "keeping staged files at %s: queued tasks still need them. "
                "Remove when the queue is empty: docker exec <worker> rm -rf %s",
                container_root,
                container_root,
            )

    outcome = summary["outcome"]
    LOG.info(
        "load done: success=%d failed=%d timeout=%d aborted=%d still_in_flight=%d "
        "over %.1fs",
        outcome["success"],
        outcome["failed"],
        outcome["timed_out"],
        outcome["aborted"],
        outcome["not_finished"],
        summary["throughput"]["elapsed_s"],
    )
    LOG.info(
        "note: queued Celery tasks keep running server-side; the API has no cancel"
    )
    return 130 if interrupted else 0


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
    ap.add_argument("--concurrency", type=int, default=4,
                    help="max files in flight (submitted but not finished)")
    ap.add_argument("--task-timeout", type=float, default=2400.0,
                    help="give up on one file after N seconds (keep above MINERU_TIMEOUT)")
    ap.add_argument("--poll-interval", type=float, default=0.5)
    ap.add_argument("--poll-max-interval", type=float, default=5.0)
    ap.add_argument("--sample-interval", type=float, default=2.0,
                    help="seconds between CPU/GPU samples")
    ap.add_argument("--throughput-interval", type=float, default=10.0,
                    help="seconds between throughput log rows")
    ap.add_argument("--window", type=float, default=60.0,
                    help="recent-throughput window in seconds")
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
    # ---- load mode (virtual users) ------------------------------------- #
    ap.add_argument("--users", type=int, default=0, metavar="N",
                    help="load mode: N virtual users, each submits a parse of "
                    "a file from the directory and exits once it has the "
                    "result (0 = one pass per file, batch mode)")
    ap.add_argument("--duration", type=float, default=0.0, metavar="SEC",
                    help="load mode: safety limit, stop after SEC seconds "
                    "(0 = run until every user got its result)")
    ap.add_argument("--ramp-up", type=float, default=0.0, metavar="SEC",
                    help="load mode: spread user starts over SEC seconds")
    ap.add_argument("--channels", type=int, default=0,
                    help="load mode: number of gRPC channels "
                    "(default: users/100 clamped to 4..256)")
    ap.add_argument("--max-inflight", type=int, default=512, metavar="N",
                    help="load mode: max RPCs started at once (client-side "
                    "queue; grpc.aio cancels calls beyond a few thousand "
                    "simultaneous ones)")
    ap.add_argument("--requests-per-user", type=int, default=1, metavar="N",
                    help="load mode: requests per user before it exits "
                    "(default 1; 0 = keep going until --duration / Ctrl-C)")
    ap.add_argument("--shuffle", action="store_true",
                    help="load mode: randomise the order files are handed out")
    ap.add_argument("--save-results", action="store_true",
                    help="load mode: also persist every successful chunk payload "
                    "(off by default: 10k users produce a lot of data)")
    ap.add_argument("--redis-service", default="redis",
                    help="compose service label of Redis, used for queue-depth logs")
    ap.add_argument("--dry-run", action="store_true",
                    help="enumerate and show the staging plan, then exit")
    return ap.parse_args(argv)


def _write_summary(
    out_dir: Path,
    run_id: str,
    args: argparse.Namespace,
    files: list[Path],
    skipped: dict[str, int],
    skipped_resume: int,
    stats: Stats,
    sampler: ResourceSampler,
    interrupted: bool,
    containers: list[str],
    container_root: str,
    staged: bool,
    staged_bytes: int,
    started_at: str,
) -> dict[str, Any]:
    snap = stats.snapshot(args.window)
    durations = sorted(stats.durations)
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
            "concurrency": args.concurrency,
            "task_timeout_s": args.task_timeout,
            "stage": args.stage,
            "container_root": container_root,
            "project": args.project,
            "service": args.service,
            "window_s": args.window,
        },
        "files": {
            "selected": len(files),
            "skipped_resume": skipped_resume,
            "skipped_by_reason": skipped,
        },
        "outcome": {
            "submitted": snap["submitted"],
            "success": snap["success"],
            "failed": snap["failed"],
            "timed_out": snap["timed_out"],
            "not_started": max(0, len(files) - snap["submitted"]),
        },
        "throughput": {
            "files_per_min_total": round(snap["files_per_min_total"], 3),
            "files_per_min_recent": round(snap["files_per_min_recent"], 3),
            "window_s": round(snap["recent_window_s"], 1),
            "elapsed_s": round(snap["elapsed_s"], 1),
        },
        "duration_s": {
            "mean": round(snap["avg_duration_s"], 3),
            "p50": _round(_percentile(durations, 0.50), 3),
            "p90": _round(_percentile(durations, 0.90), 3),
            "p95": _round(_percentile(durations, 0.95), 3),
            "max": _round(max(durations), 3) if durations else None,
            "sum": round(sum(durations), 3),
        },
        "work": {"chunks": snap["chunks"], "pages": snap["pages"]},
        "resources": sampler.overall_peaks(),
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
    if args.resume and args.users > 0:
        LOG.warning(
            "--resume is batch-mode only; ignoring it with --users "
            "(load mode reuses every file by design)"
        )
    elif args.resume:
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
                container_root = mounted
                LOG.info(
                    "dry run: %d worker(s) already mount %s at %s; staging would be skipped",
                    len(containers),
                    root,
                    mounted,
                )
            else:
                LOG.info("dry run: would stage into %d worker(s): %s",
                         len(containers), ", ".join(c[:12] for c in containers) or "-")
        if args.users > 0:
            LOG.info(
                "dry run: load mode would drive %d user(s) for %s over %d file(s), "
                "ramp-up %.0fs, %s",
                args.users,
                "Ctrl-C" if args.duration <= 0 else f"{args.duration:.0f}s",
                len(files),
                args.ramp_up,
                f"{args.requests_per_user} request(s)/user"
                if args.requests_per_user
                else "until stopped",
            )
        else:
            LOG.info("dry run: would submit %d file(s) as file://%s/<rel>",
                     len(files), container_root)
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
    stub = ParseServiceStub(channel)

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
            channel.close()
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
                channel.close()
                return 130
            except RuntimeError as exc:
                LOG.error("staging failed: %s", exc)
                channel.close()
                return 2
            staged = True
    else:
        LOG.info(
            "staging skipped; sources are read as file://%s/<rel> inside the workers",
            container_root,
        )

    if args.users > 0:
        channel.close()
        return run_load(
            args,
            run_id,
            started_at,
            out_dir,
            files,
            skipped,
            containers,
            container_root,
            staged,
            staged_bytes if staged else 0,
        )

    stats = Stats()
    sampler = ResourceSampler(args.sample_interval)
    sampler.start()
    reporter = ThroughputReporter(
        run_id, stats, sampler, out_dir / "throughput.jsonl",
        args.throughput_interval, args.window,
    )
    reporter.start()

    runner = Runner(args, run_id, stub, stats, sampler, out_dir, container_root)
    executor = ThreadPoolExecutor(max_workers=args.concurrency, thread_name_prefix="submit")
    interrupted = False
    try:
        futures = {
            executor.submit(runner.run_one, idx, len(files), path): path
            for idx, path in enumerate(files)
        }
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception:  # noqa: BLE001 - run_one catches internally; this is a bug
                LOG.exception("unexpected worker error for %s", futures[fut])
    except KeyboardInterrupt:
        interrupted = True
        LOG.warning(
            "interrupted: cancelling queued files; in-flight requests finish before exit"
        )
        executor.shutdown(wait=False, cancel_futures=True)
    finally:
        if not interrupted:
            executor.shutdown(wait=True)
        sampler.stop()
        reporter.stop()
        try:
            reporter.report_once()
        except Exception:  # noqa: BLE001
            LOG.exception("final throughput row failed")

        summary = _write_summary(
            out_dir,
            run_id,
            args,
            files,
            skipped,
            skipped_resume,
            stats,
            sampler,
            interrupted,
            containers,
            container_root,
            staged,
            staged_bytes if staged else 0,
            started_at,
        )

        # Only delete a directory this script created for this run; a
        # user-provided --container-root is left alone.
        if (
            staged
            and containers
            and not args.container_root
            and not args.keep_staged
        ):
            cleanup_staged(containers, container_root)

        channel.close()

    outcome = summary["outcome"]
    LOG.info(
        "done: success=%d failed=%d timeout=%d not_started=%d in %.1fs",
        outcome["success"],
        outcome["failed"],
        outcome["timed_out"],
        outcome["not_started"],
        summary["throughput"]["elapsed_s"],
    )
    LOG.info("results: %s", out_dir.resolve())
    return 0 if outcome["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
