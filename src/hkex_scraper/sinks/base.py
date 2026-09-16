"""Uniform persistence-sink contract.

A *sink* is one output destination for scraped records (PostgreSQL, MySQL,
SQLite, SurrealDB, …). The pipeline, graph linker, and CLI speak only this
contract; they never branch on a destination name.

Every method returns ``(value, error_code)`` where ``""`` means success. No
method raises for a driver or connection problem: those are returned as codes so
one sink can never abort a write to another. Capability differences (native
upsert, JSON, arrays, text limits) are declared in :class:`SinkCapabilities`
rather than silently assumed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Normalized error codes
# ---------------------------------------------------------------------------
ERR_NONE = ""
SUFFIX_DISABLED = "DISABLED"
SUFFIX_DRIVER_MISSING = "DRIVER_MISSING"
SUFFIX_DSN_MISSING = "DSN_MISSING"
SUFFIX_CONNECT_FAILED = "CONNECT_FAILED"
SUFFIX_SCHEMA_ERROR = "SCHEMA_ERROR"
SUFFIX_WRITE_ERROR = "WRITE_ERROR"
SUFFIX_PAYLOAD_ERROR = "PAYLOAD_ERROR"
SUFFIX_PARITY_MISMATCH = "PARITY_MISMATCH"
SUFFIX_UNSUPPORTED = "UNSUPPORTED"


def code(sink_id: str, suffix: str) -> str:
    """Build a namespaced error code, e.g. ``MYSQL_DRIVER_MISSING``."""
    return f"{sink_id.upper()}_{suffix}" if suffix else ERR_NONE


# ---------------------------------------------------------------------------
# Credential redaction
# ---------------------------------------------------------------------------
_DSN_CRED_RE = re.compile(r"(?i)(password|pwd)=([^\s]+)")
_URL_CRED_RE = re.compile(r"(?i)(://[^:/@\s]+):([^@\s]+)@")


def redact(text: str) -> str:
    """Remove credentials from error text before it is logged."""
    if not text:
        return ""
    redacted = _DSN_CRED_RE.sub(r"\1=***", text)
    redacted = _URL_CRED_RE.sub(r"\1:***@", redacted)
    return redacted


# ---------------------------------------------------------------------------
# Capabilities
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class SinkCapabilities:
    """Declared, machine-readable differences between sinks."""

    model: str = "relational"  # relational | document | graph
    native_upsert: bool = True
    reads: bool = True
    edges: bool = True
    json: bool = False
    arrays: bool = False
    transactions: bool = False
    bulk: bool = True
    text_limit: Optional[int] = None


# ---------------------------------------------------------------------------
# Sink base
# ---------------------------------------------------------------------------
class Sink:
    """Base class for persistence sinks.

    Subclasses must set :attr:`id` and :attr:`capabilities` and override the
    write methods they support. The read methods default to an ``UNSUPPORTED``
    error so a non-reading sink fails loudly rather than returning empty data.
    """

    id: str = "sink"
    capabilities: SinkCapabilities = SinkCapabilities()

    # -- lifecycle ---------------------------------------------------------
    def available(self) -> bool:
        """True when the driver is importable and connection details are present."""
        raise NotImplementedError

    def unavailable_reason(self) -> str:
        """Actionable text naming the missing driver extra or setting."""
        return f"{self.id} sink is unavailable"

    def ensure_schema(self) -> Tuple[bool, str]:
        """Create the sink's tables/indexes idempotently."""
        return True, ERR_NONE

    def close(self) -> None:
        """Release any pooled connections. Best effort; never raises."""

    # -- writes ------------------------------------------------------------
    def upsert_filings(self, records: List[Dict[str, Any]]) -> Tuple[int, str]:
        return 0, code(self.id, SUFFIX_UNSUPPORTED)

    def upsert_document(self, filing_id: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
        return False, code(self.id, SUFFIX_UNSUPPORTED)

    def mark_status(self, filing_id: str, status: str, reason: str = "") -> Tuple[bool, str]:
        return False, code(self.id, SUFFIX_UNSUPPORTED)

    def insert_coverage(self, chunk: Dict[str, Any]) -> Tuple[bool, str]:
        return False, code(self.id, SUFFIX_UNSUPPORTED)

    def upsert_edges(self, edges: List[Dict[str, Any]], kind: str) -> Tuple[int, str]:
        return 0, code(self.id, SUFFIX_UNSUPPORTED)

    # -- reads -------------------------------------------------------------
    def count_filings(self) -> Tuple[int, str]:
        return 0, code(self.id, SUFFIX_UNSUPPORTED)

    def count_edges(self, kind: str) -> Tuple[int, str]:
        return 0, code(self.id, SUFFIX_UNSUPPORTED)

    def count_pending_filings(self) -> Tuple[int, str]:
        return 0, code(self.id, SUFFIX_UNSUPPORTED)

    def fetch_pending_filings(self, limit: int) -> Tuple[List[Dict[str, Any]], str]:
        return [], code(self.id, SUFFIX_UNSUPPORTED)

    def distinct_company_tickers(self) -> Tuple[List[str], str]:
        return [], code(self.id, SUFFIX_UNSUPPORTED)

    def fetch_filing_ids_by_ticker(self, tickers: List[str]) -> Tuple[List[Dict[str, Any]], str]:
        return [], code(self.id, SUFFIX_UNSUPPORTED)

    def fetch_titles(
        self, ticker_set: Optional[List[str]], offset: int, page_size: int
    ) -> Tuple[List[Dict[str, Any]], str]:
        return [], code(self.id, SUFFIX_UNSUPPORTED)

    def fetch_coverage(self) -> Tuple[List[Dict[str, Any]], str]:
        return [], code(self.id, SUFFIX_UNSUPPORTED)


# Edge kinds accepted by ``upsert_edges``.
EDGE_KINDS = ("has_filing", "references_filing")
