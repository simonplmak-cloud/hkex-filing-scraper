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
    text_search: bool = True
    snippets: bool = False


# ---------------------------------------------------------------------------
# Query surface
# ---------------------------------------------------------------------------
# Accepted ``FilingQuery.order_by`` values and ``aggregate_filings`` group-bys.
SEARCH_ORDER_BY = ("filing_date_desc", "filing_date_asc", "title_asc", "filing_id_asc")
AGGREGATE_GROUPS = (
    "company_ticker",
    "filing_type",
    "filing_category",
    "document_status",
    "exchange",
)

# Read columns returned by ``search_filings`` / ``search_documents`` (never document_text).
SEARCH_COLUMNS = (
    "filing_id",
    "company_ticker",
    "stock_code",
    "stock_name",
    "exchange",
    "filing_type",
    "filing_subtype",
    "filing_category",
    "title",
    "filing_date",
    "document_url",
    "referenced_tickers",
    "source",
    "updated_at",
    "document_status",
    "document_type",
    "document_text_len",
    "document_table_cnt",
)


@dataclass(frozen=True)
class FilingQuery:
    """A composable, sink-agnostic filing filter.

    Every field is optional; an empty value is ignored. ``document_status`` accepts the
    synthetic value ``"unprocessed"`` (meaning ``document_status IS NULL``) alongside the
    real statuses. ``date_from``/``date_to`` are ISO ``YYYY-MM-DD`` strings (inclusive).
    """

    tickers: Tuple[str, ...] = ()
    stock_codes: Tuple[str, ...] = ()
    title_query: str = ""
    text_query: str = ""
    filing_types: Tuple[str, ...] = ()
    filing_categories: Tuple[str, ...] = ()
    document_status: Tuple[str, ...] = ()
    exchange: str = ""
    referenced_ticker: str = ""
    source: str = ""
    document_type: str = ""
    date_from: str = ""
    date_to: str = ""
    order_by: str = "filing_date_desc"


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
    def read_filing_digests(self) -> Tuple[List[Dict[str, Any]], str]:
        """Return ``[{"filing_id": ..., "document_sha256": ...}]`` for every filing.

        Optional capability: used by ``--verify`` to compare sinks semantically. Sinks that
        cannot enumerate their records return an explicit ``UNSUPPORTED`` code rather than an
        empty list, so a caller can tell "no data" from "cannot tell".
        """
        return [], code(self.id, SUFFIX_UNSUPPORTED)

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
        self,
        ticker_set: Optional[List[str]],
        offset: int,
        page_size: int,
        title_query: str = "",
    ) -> Tuple[List[Dict[str, Any]], str]:
        """Page through filings for cross-reference scanning or title search.

        ``title_query`` is an optional case-insensitive substring filter on the title.
        It is additive: existing callers pass only the first three arguments.
        """
        return [], code(self.id, SUFFIX_UNSUPPORTED)

    def fetch_filing_detail(self, filing_id: str) -> Tuple[Optional[Dict[str, Any]], str]:
        """Return one filing's metadata plus extracted document fields.

        Returns ``(None, "")`` when the id is not found and ``(None, code)`` on a
        driver/connection error, so a caller can tell "no such filing" from "cannot read".
        Optional capability: a sink that cannot serve it returns ``UNSUPPORTED``.
        """
        return None, code(self.id, SUFFIX_UNSUPPORTED)

    def search_filings(
        self, query: FilingQuery, offset: int, limit: int
    ) -> Tuple[List[Dict[str, Any]], str]:
        """Return rich filing summaries matching *query*, newest-first by default.

        Rows carry every filing field plus document status/size summary columns, and never
        ``document_text``. Optional capability: returns ``UNSUPPORTED`` when absent.
        """
        return [], code(self.id, SUFFIX_UNSUPPORTED)

    def search_documents(
        self, query: FilingQuery, offset: int, limit: int
    ) -> Tuple[List[Dict[str, Any]], str]:
        """Full-text search over ``document_text``; adds a ``snippet`` when supported."""
        return [], code(self.id, SUFFIX_UNSUPPORTED)

    def aggregate_filings(
        self, group_by: str, query: FilingQuery
    ) -> Tuple[List[Dict[str, Any]], str]:
        """Group matching filings by ``group_by``; returns ``[{key, count}]`` by count desc."""
        return [], code(self.id, SUFFIX_UNSUPPORTED)

    def list_companies(self, limit: int, offset: int) -> Tuple[List[Dict[str, Any]], str]:
        """Return distinct companies with a display name and filing count."""
        return [], code(self.id, SUFFIX_UNSUPPORTED)

    def fetch_coverage(self) -> Tuple[List[Dict[str, Any]], str]:
        return [], code(self.id, SUFFIX_UNSUPPORTED)


# Edge kinds accepted by ``upsert_edges``.
EDGE_KINDS = ("has_filing", "references_filing")
