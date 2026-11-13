"""Document extraction port.

Abstracts "turn raw document bytes into structured content blocks" behind an
interface. The concrete engine (MinerU via its HTTP API, a cloud service, or
an SDK) is a replaceable adapter; the core parsing logic only consumes the
canonical `content_list` shape.

The filename matters: engines such as MinerU pick their parser from the file
extension, so adapters receive it alongside the bytes.

`content_list` is a flat list of blocks already ordered by reading order, each
a dict carrying at least:
  - `type`:  "text" | "title" | "image" | "chart" | "table" | "equation"
             | "code" | "list" | "ref_text"
  - `page_idx`: 0-based page number
  and, per type:
  - `content`: plain text (equations as LaTeX); for tables, the engine's
    best-effort markdown rendering, used only as a fallback;
  - `captions`: `list[str]` — figure/table caption text, already flattened
    from whatever structure the engine uses;
  - `table_body`: table-only, HTML with `rowspan`/`colspan` intact. Preferred
    over `content` because flattening spans first destroys multi-level headers.

Adapters are responsible for normalising the engine's native field names into
this shape; the domain layer relies on it alone.
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class DocumentExtractError(Exception):
    """Failed to extract structured content from a document."""


class DocumentExtractor(ABC):
    """Extract structured content blocks from raw document bytes."""

    @abstractmethod
    def extract(self, data: bytes, filename: str) -> list[dict]:
        """Return the flat `content_list` (reading-ordered blocks) for a document.

        `filename` is a bare basename (no directories); engines use its
        extension to select a parser. Raises DocumentExtractError on failure.
        """
        raise NotImplementedError

    def health(self) -> bool:
        """Best-effort liveness probe of the backing engine; never raises.

        The default assumes the engine is reachable. Adapters that talk to an
        independent service (the MinerU HTTP client) override it so callers
        can defer work while that service is down.
        """
        return True
