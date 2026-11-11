"""MinerU adapter: parse documents via the MinerU V1 Tasks API.

MinerU 4.1's `mineru-kit api-server` is the GPU parsing engine and exposes an
asynchronous Tasks API. This adapter drives it directly instead of going
through the optional `file-parse-server` synchronous wrapper, so the stack
needs only one MinerU container. Per document:

    POST /v1/tasks                create a one-file task (JSON), 202
    PUT  {upload.url}             upload the raw bytes the response points at
    GET  /v1/tasks/{task_id}      poll until the file reaches a terminal state
    GET  {result_url}             download the result archive

The `default` output format is a ZIP containing markdown, middle JSON and
`structured_content.json`; the last two are what this adapter consumes.

Two MinerU quirks are absorbed here so the domain layer never sees them:

- figure/table captions arrive as `[{"bbox": ..., "content": ...}]`, not as
  the flat strings the port documents — normalised to `captions: list[str]`;
- `structured_content` renders tables to markdown *after* dropping
  rowspan/colspan, which mangles multi-level headers. `middle_json.json` in
  the same ZIP still has the original HTML table body, so it is attached back
  as `table_body` (matched by page + bbox) for the chunker to re-render.
"""
from __future__ import annotations

import io
import json
import logging
import time
import zipfile
from typing import Any
from urllib.parse import quote

import httpx

from ...ports.document_extractor import DocumentExtractError, DocumentExtractor

logger = logging.getLogger("py_parse.mineru")

_TASKS_PATH = "/v1/tasks"
_HEALTH_PATH = "/v1/health"
_STRUCTURED_CONTENT_MEMBER = "structured_content.json"
_MIDDLE_JSON_MEMBER = "middle_json.json"
_OUTPUT_FORMATS = "default"
# MinerU file states after which polling must stop.
_COMPLETED_STATES = frozenset({"completed"})
_FAILED_STATES = frozenset({"failed", "canceled"})
# Caption fields MinerU has used across versions; first non-empty one wins.
_CAPTION_KEYS = ("captions", "caption", "image_caption", "table_caption")


class MineruClient(DocumentExtractor):
    """HTTP client that manages one MinerU parse task per document."""

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        *,
        job_timeout: float = 1800.0,
        http_timeout: float = 30.0,
        poll_initial: float = 0.5,
        poll_max: float = 5.0,
        max_connections: int = 1,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        # Overall deadline for one document (create + upload + parse + download).
        # Kept separate from `http_timeout`, which only bounds a single request.
        self._job_timeout = job_timeout
        self._poll_initial = poll_initial
        self._poll_max = poll_max
        # One worker handles one task at a time, so one connection is enough;
        # raise this only if the caller drives extract() concurrently.
        self._client = httpx.Client(
            timeout=http_timeout,
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_connections,
            ),
        )

    # -- DocumentExtractor -------------------------------------------------

    def extract(self, data: bytes, filename: str) -> list[dict]:
        deadline = time.monotonic() + self._job_timeout
        task_id, upload = self._create_task(_normalize_filename(filename), len(data))
        try:
            self._upload(upload, data)
            result_url = self._wait_for_result(task_id, deadline)
            blob, content_type = self._download(result_url)
        except BaseException:
            # Timeout, interrupt or worker shutdown: don't let the GPU keep
            # parsing something nobody will collect.
            self._cancel(task_id)
            raise

        structured_content, middle_json = self._bundle(blob, content_type)
        blocks = self._flatten(structured_content)
        self._normalize_captions(blocks)
        self._attach_table_html(blocks, middle_json)
        return blocks

    # -- Task lifecycle ----------------------------------------------------

    def _create_task(self, filename: str, size: int) -> tuple[str, dict]:
        payload = {
            "files": [{"file_name": filename, "bytes": size}],
            "output_formats": [_OUTPUT_FORMATS],
        }
        try:
            resp = self._client.post(f"{self._base_url}{_TASKS_PATH}", json=payload)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            raise DocumentExtractError(f"mineru-api create task failed: {e}") from e

        try:
            body = resp.json()
            task_id = body["task_id"]
            entry = body["files"][0]
        except (ValueError, KeyError, IndexError, TypeError) as e:
            raise DocumentExtractError("unexpected mineru-api create response") from e

        upload = entry.get("upload") if isinstance(entry, dict) else None
        if not isinstance(upload, dict) or not upload.get("url"):
            status = entry.get("status") if isinstance(entry, dict) else None
            raise DocumentExtractError(
                f"mineru-api did not return an upload URL (file status {status!r})"
            )
        return task_id, upload

    def _upload(self, upload: dict, data: bytes) -> None:
        # The server hands back the exact URL and headers; only the content
        # type is ours. A successful upload closes the connection (large-file
        # relay), which httpx handles transparently.
        headers = {str(k): str(v) for k, v in (upload.get("headers") or {}).items()}
        headers["Content-Type"] = "application/octet-stream"
        try:
            resp = self._client.put(upload["url"], content=data, headers=headers)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            raise DocumentExtractError(f"mineru-api upload failed: {e}") from e

    def _wait_for_result(self, task_id: str, deadline: float) -> str:
        url = f"{self._base_url}{_TASKS_PATH}/{quote(task_id, safe='')}"
        delay = self._poll_initial
        while True:
            detail = self._poll_once(url)

            if detail is not None:
                entry = self._single_file(detail)
                status = entry.get("status")
                if status in _COMPLETED_STATES:
                    result_url = entry.get("result_url")
                    if not result_url:
                        raise DocumentExtractError(
                            f"mineru-api task {task_id} completed without a result_url"
                        )
                    return str(result_url)
                if status in _FAILED_STATES:
                    raise DocumentExtractError(self._task_error(detail, entry, status))

            if time.monotonic() >= deadline:
                raise DocumentExtractError(
                    f"mineru-api task {task_id} did not finish within "
                    f"{self._job_timeout:.0f}s"
                )

            time.sleep(delay)
            delay = min(delay * 1.5, self._poll_max)

    def _poll_once(self, url: str) -> dict | None:
        """GET the task detail; return None for retryable failures.

        A missing/failed poll is not fatal on its own: the task keeps running
        server-side, so transport errors, 429 and 5xx just mean "try again"
        until the deadline. 4xx errors are permanent (unknown task, bad auth).
        """
        try:
            resp = self._client.get(url)
            resp.raise_for_status()
            detail = resp.json()
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            if code >= 500 or code == 429:
                logger.warning("mineru poll failed (HTTP %s); retrying", code)
                return None
            raise DocumentExtractError(f"mineru-api poll failed: {e}") from e
        except (httpx.HTTPError, ValueError) as e:
            logger.warning("mineru poll failed (%s); retrying", e)
            return None

        if not isinstance(detail, dict):
            raise DocumentExtractError("unexpected mineru-api task detail")
        return detail

    def _download(self, result_url: str) -> tuple[bytes, str]:
        url = result_url if "://" in result_url else f"{self._base_url}{result_url}"
        try:
            resp = self._client.get(url)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            raise DocumentExtractError(f"mineru-api result download failed: {e}") from e
        return resp.content, resp.headers.get("content-type", "")

    def _cancel(self, task_id: str) -> None:
        """Best-effort cancel; never masks the original exception."""
        try:
            self._client.post(
                f"{self._base_url}{_TASKS_PATH}/{quote(task_id, safe='')}/cancel"
            )
        except httpx.HTTPError:
            logger.debug("failed to cancel mineru task %s", task_id, exc_info=True)

    @staticmethod
    def _single_file(detail: dict) -> dict:
        files = detail.get("files")
        if not isinstance(files, list) or not files:
            raise DocumentExtractError("mineru-api task detail has no files")
        entry = files[0]
        if not isinstance(entry, dict):
            raise DocumentExtractError("unexpected mineru-api file entry")
        return entry

    @staticmethod
    def _task_error(detail: dict, entry: dict, status: Any) -> str:
        for holder in (entry, detail):
            error = holder.get("error") if isinstance(holder, dict) else None
            if isinstance(error, dict) and error.get("message"):
                code = error.get("code")
                return f"mineru-api task {status}: {error['message']} ({code})"
        return f"mineru-api task ended with status {status!r}"

    # -- Response decoding -------------------------------------------------

    def _bundle(self, blob: bytes, content_type: str) -> tuple[dict, dict | None]:
        """Pull (structured_content, middle_json) out of the ZIP attachment.

        `middle_json` is optional: it is only used to recover table HTML, so a
        missing member degrades to the markdown fallback instead of failing.
        """
        if "zip" not in content_type and blob[:2] != b"PK":
            raise DocumentExtractError(
                f"unexpected mineru-api result content-type {content_type!r}; "
                "expected a ZIP archive"
            )
        try:
            with zipfile.ZipFile(io.BytesIO(blob)) as archive:
                names = archive.namelist()
                structured = self._member(archive, names, _STRUCTURED_CONTENT_MEMBER)
                if structured is None:
                    raise DocumentExtractError(
                        f"mineru-api ZIP has no {_STRUCTURED_CONTENT_MEMBER}: {names}"
                    )
                middle_name = self._find_member(names, _MIDDLE_JSON_MEMBER)
                middle = (
                    json.loads(archive.read(middle_name)) if middle_name else None
                )
                return json.loads(structured), middle
        except zipfile.BadZipFile as e:
            raise DocumentExtractError("mineru-api returned a corrupt ZIP") from e

    @staticmethod
    def _find_member(names: list[str], basename: str) -> str | None:
        for name in names:
            if name.rsplit("/", 1)[-1] == basename:
                return name
        return None

    @classmethod
    def _member(cls, archive: zipfile.ZipFile, names: list[str], basename: str):
        found = cls._find_member(names, basename)
        return archive.read(found) if found else None

    @classmethod
    def _normalize_captions(cls, blocks: list[dict]) -> None:
        """Flatten `captions: [{"bbox": .., "content": ..}]` to `list[str]`."""
        for block in blocks:
            for key in _CAPTION_KEYS:
                raw = block.get(key)
                if not raw:
                    continue
                items = raw if isinstance(raw, list) else [raw]
                texts: list[str] = []
                for item in items:
                    if isinstance(item, dict):
                        item = item.get("content") or item.get("text") or ""
                    item = str(item).strip()
                    if item:
                        texts.append(item)
                if texts:
                    block["captions"] = texts
                    break

    @classmethod
    def _attach_table_html(cls, blocks: list[dict], middle: dict | None) -> None:
        """Attach `middle_json`'s rowspan/colspan-preserving table HTML."""
        if not isinstance(middle, dict):
            return
        html_by_key: dict[tuple, str] = {}
        for page in middle.get("pages") or []:
            page_idx = int(page.get("page_idx", 0))
            for block in page.get("blocks") or []:
                if block.get("type") != "table":
                    continue
                key = cls._bbox_key(page_idx, block.get("bbox"))
                body = next(
                    (
                        part for part in block.get("content") or []
                        if isinstance(part, dict) and part.get("type") == "table_body"
                    ),
                    None,
                )
                if key is not None and body and body.get("content"):
                    html_by_key[key] = body["content"]
        if not html_by_key:
            return
        for block in blocks:
            if block.get("type") != "table":
                continue
            key = cls._bbox_key(int(block.get("page_idx", 0)), block.get("bbox"))
            if key is None:
                continue
            html = html_by_key.get(key)
            if html:
                block["table_body"] = html

    @staticmethod
    def _bbox_key(page_idx: int, bbox) -> tuple | None:
        try:
            return (page_idx, tuple(round(float(v), 4) for v in bbox))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _flatten(payload: Any) -> list[dict]:
        """Flatten `structured_content.json` `pages[].blocks[]` into block dicts."""
        if not isinstance(payload, dict):
            raise DocumentExtractError(
                f"unexpected structured_content: expected object, got {type(payload).__name__}"
            )
        try:
            pages = payload["pages"]
        except (KeyError, TypeError) as e:
            raise DocumentExtractError(
                "unexpected structured_content shape: missing pages"
            ) from e

        blocks: list[dict] = []
        for page in pages:
            page_idx = int(page.get("page_idx", 0))
            for block in page.get("blocks", []):
                flat = dict(block)
                flat["page_idx"] = page_idx
                blocks.append(flat)
        return blocks

    # -- Probes / teardown -------------------------------------------------

    def health(self) -> bool:
        """Best-effort liveness probe; returns False instead of raising."""
        try:
            return self._client.get(f"{self._base_url}{_HEALTH_PATH}").is_success
        except httpx.HTTPError:
            return False

    def close(self) -> None:
        """Release the underlying HTTP client (call when no longer needed)."""
        self._client.close()


def _normalize_filename(filename: str) -> str:
    """Reduce to the bare basename MinerU accepts, keeping its extension."""
    name = filename.replace("\\", "/").rsplit("/", 1)[-1].strip()
    name = "".join(ch for ch in name if ch >= " " and ch != ":")
    return name or "document.pdf"
