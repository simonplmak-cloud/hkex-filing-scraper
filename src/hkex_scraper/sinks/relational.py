"""Shared relational-sink engine.

One loop — connect, bind parameters, count, redact, degrade — drives every SQL
dialect. Engine-specific connection handling lives in the thin driver adapters
(``sqlite.py``, ``mysql.py``); syntax lives in the :class:`Dialect`.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .base import (
    EDGE_KINDS,
    ERR_NONE,
    SUFFIX_CONNECT_FAILED,
    SUFFIX_DRIVER_MISSING,
    SUFFIX_DSN_MISSING,
    SUFFIX_PAYLOAD_ERROR,
    SUFFIX_SCHEMA_ERROR,
    SUFFIX_WRITE_ERROR,
    Sink,
    SinkCapabilities,
    code,
    redact,
)
from .dialects import (
    COVERAGE_COLUMNS,
    DOCUMENT_COLUMNS,
    EDGE_KEYS,
    FILING_COLUMNS,
    Dialect,
)

from ..utils import ticker_to_record_id


class RelationalDriver:
    """Minimal DB-API-shaped driver used by :class:`RelationalSink`."""

    def connect(self) -> None:
        """Open the connection (idempotent). Raises on failure."""
        raise NotImplementedError

    def execute(
        self, sql: str, params: Optional[Any] = None, many: bool = False
    ) -> Tuple[bool, str, int]:
        raise NotImplementedError

    def fetch_all(self, sql: str, params: Optional[Any] = None) -> Tuple[List[Dict[str, Any]], str]:
        raise NotImplementedError

    def fetch_scalar(self, sql: str, params: Optional[Any] = None) -> Tuple[Any, str]:
        raise NotImplementedError

    def close(self) -> None:
        """Release the connection. Best effort; never raises."""


class RelationalSink(Sink):
    """A sink backed by a SQL dialect and a driver adapter."""

    capabilities = SinkCapabilities(
        model="relational", native_upsert=True, reads=True, edges=True, json=True, arrays=False
    )

    def __init__(self, sink_id: str, dialect: Dialect, driver: RelationalDriver) -> None:
        self.id = sink_id
        self.dialect = dialect
        self._driver_obj = driver
        self._driver_ready = False

    # -- subclass hooks ----------------------------------------------------
    def driver_installed(self) -> bool:
        raise NotImplementedError

    def configured(self) -> bool:
        raise NotImplementedError

    def unavailable_reason(self) -> str:
        if not self.driver_installed():
            return f"{self.id} sink requires an optional driver that is not installed"
        if not self.configured():
            return f"{self.id} sink is missing its connection settings"
        return f"{self.id} sink is unavailable"

    # -- lifecycle ---------------------------------------------------------
    def available(self) -> bool:
        return self.driver_installed() and self.configured()

    def _ensure_driver(self) -> Tuple[Optional[RelationalDriver], str]:
        if not self.driver_installed():
            return None, code(self.id, SUFFIX_DRIVER_MISSING)
        if not self.configured():
            return None, code(self.id, SUFFIX_DSN_MISSING)
        if not self._driver_ready:
            try:
                self._driver_obj.connect()
            except Exception as exc:  # noqa: BLE001 - surfaced as a code
                return None, redact(str(exc)) or code(self.id, SUFFIX_CONNECT_FAILED)
            self._driver_ready = True
        return self._driver_obj, ERR_NONE

    def ensure_schema(self) -> Tuple[bool, str]:
        driver, err = self._ensure_driver()
        if err:
            return False, err
        assert driver is not None
        for statement in self.dialect.ddl():
            ok, err_code, _ = driver.execute(statement)
            if not ok:
                return False, err_code or code(self.id, SUFFIX_SCHEMA_ERROR)
        return True, ERR_NONE

    def close(self) -> None:
        if self._driver_ready:
            self._driver_obj.close()
            self._driver_ready = False

    # -- row builders ------------------------------------------------------
    @staticmethod
    def _filing_row(filing: Dict[str, Any]) -> Tuple[Any, ...]:
        return (
            filing.get("filing_id", ""),
            filing.get("company_ticker", ""),
            filing.get("stock_code", ""),
            filing.get("stock_name"),
            filing.get("exchange", "HK"),
            filing.get("filing_type", "Other"),
            filing.get("filing_subtype"),
            filing.get("filing_category"),
            filing.get("title"),
            filing.get("filing_date"),
            filing.get("document_url"),
            filing.get("referenced_tickers"),
            filing.get("source", "HKEx"),
            filing.get("updated_at") or datetime.now(timezone.utc),
        )

    def _document_row(self, filing_id: str, payload: Dict[str, Any]) -> Tuple[Any, ...]:
        return (
            payload.get("document_size"),
            payload.get("document_type"),
            payload.get("document_hash"),
            payload.get("document_sha256"),
            payload.get("document_text"),
            payload.get("document_text_len"),
            payload.get("document_tables"),
            payload.get("document_table_cnt"),
            payload.get("document_status"),
            payload.get("document_status_reason"),
            datetime.now(timezone.utc),
            filing_id,
        )

    @staticmethod
    def _coverage_row(chunk: Dict[str, Any]) -> Tuple[Any, ...]:
        return (
            chunk.get("chunk_from"),
            chunk.get("chunk_to"),
            chunk.get("api_count", 0),
            chunk.get("ingested_count", 0),
            chunk.get("unique_count", 0),
            chunk.get("run_id", ""),
            chunk.get("timestamp") or datetime.now(timezone.utc),
        )

    @staticmethod
    def _edge_row(kind: str, edge: Dict[str, Any]) -> Tuple[Any, ...]:
        company_id = edge.get("company_id") or ticker_to_record_id(edge.get("company_ticker", ""))
        if kind == "has_filing":
            return (company_id, edge.get("filing_id", ""))
        return (
            edge.get("filing_id", ""),
            company_id,
            edge.get("source", "title_extraction"),
        )

    # -- writes ------------------------------------------------------------
    def upsert_filings(self, records: List[Dict[str, Any]]) -> Tuple[int, str]:
        if not records:
            return 0, ERR_NONE
        driver, err = self._ensure_driver()
        if err:
            return 0, err
        assert driver is not None
        try:
            rows = [self.dialect.encode_row(self._filing_row(r)) for r in records]
        except Exception as exc:  # noqa: BLE001
            return 0, redact(str(exc)) or code(self.id, SUFFIX_PAYLOAD_ERROR)
        ok, err_code, affected = driver.execute(self.dialect.upsert_filings_sql(), rows, many=True)
        if not ok:
            return 0, err_code or code(self.id, SUFFIX_WRITE_ERROR)
        return affected, ERR_NONE

    def upsert_document(self, filing_id: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
        driver, err = self._ensure_driver()
        if err:
            return False, err
        assert driver is not None
        params = self.dialect.encode_row(self._document_row(filing_id, payload))
        ok, err_code, rowcount = driver.execute(self.dialect.upsert_document_sql(), params)
        if not ok:
            return False, err_code or code(self.id, SUFFIX_WRITE_ERROR)
        if rowcount == 0:
            return False, code(self.id, SUFFIX_WRITE_ERROR)
        return True, ERR_NONE

    def mark_status(self, filing_id: str, status: str, reason: str = "") -> Tuple[bool, str]:
        driver, err = self._ensure_driver()
        if err:
            return False, err
        assert driver is not None
        params = self.dialect.encode_row(
            [status, reason or "", datetime.now(timezone.utc), filing_id]
        )
        ok, err_code, rowcount = driver.execute(self.dialect.mark_status_sql(), params)
        if not ok:
            return False, err_code or code(self.id, SUFFIX_WRITE_ERROR)
        if rowcount == 0:
            return False, code(self.id, SUFFIX_WRITE_ERROR)
        return True, ERR_NONE

    def insert_coverage(self, chunk: Dict[str, Any]) -> Tuple[bool, str]:
        driver, err = self._ensure_driver()
        if err:
            return False, err
        assert driver is not None
        params = self.dialect.encode_row(self._coverage_row(chunk))
        ok, err_code, _ = driver.execute(self.dialect.insert_coverage_sql(), params)
        if not ok:
            return False, err_code or code(self.id, SUFFIX_WRITE_ERROR)
        return True, ERR_NONE

    def upsert_edges(self, edges: List[Dict[str, Any]], kind: str) -> Tuple[int, str]:
        if kind not in EDGE_KINDS:
            return 0, code(self.id, SUFFIX_PAYLOAD_ERROR)
        if not edges:
            return 0, ERR_NONE
        driver, err = self._ensure_driver()
        if err:
            return 0, err
        assert driver is not None
        sql = self.dialect.upsert_edge_sql(kind)
        created = 0
        for edge in edges:
            params = self.dialect.encode_row(self._edge_row(kind, edge))
            ok, err_code, rowcount = driver.execute(sql, params)
            if not ok:
                return created, err_code or code(self.id, SUFFIX_WRITE_ERROR)
            if rowcount and rowcount > 0:
                created += rowcount
        return created, ERR_NONE

    # -- reads -------------------------------------------------------------
    def _read(self, sql: str, params: Optional[Any] = None) -> Tuple[List[Dict[str, Any]], str]:
        driver, err = self._ensure_driver()
        if err:
            return [], err
        assert driver is not None
        return driver.fetch_all(sql, params)

    def _read_scalar(self, sql: str, params: Optional[Any] = None) -> Tuple[Any, str]:
        driver, err = self._ensure_driver()
        if err:
            return None, err
        assert driver is not None
        return driver.fetch_scalar(sql, params)

    def _count(self, sql: str, params: Optional[Any] = None) -> Tuple[int, str]:
        value, err = self._read_scalar(sql, params)
        if err:
            return 0, err
        try:
            return int(value or 0), ERR_NONE
        except (TypeError, ValueError):
            return 0, code(self.id, SUFFIX_WRITE_ERROR)

    def count_filings(self) -> Tuple[int, str]:
        return self._count(self.dialect.count_filings_sql())

    def count_edges(self, kind: str) -> Tuple[int, str]:
        if kind not in EDGE_KINDS:
            return 0, code(self.id, SUFFIX_PAYLOAD_ERROR)
        return self._count(self.dialect.count_edges_sql(kind))

    def count_pending_filings(self) -> Tuple[int, str]:
        return self._count(self.dialect.count_pending_sql())

    def fetch_pending_filings(self, limit: int) -> Tuple[List[Dict[str, Any]], str]:
        return self._read(self.dialect.fetch_pending_sql(), [limit])

    def distinct_company_tickers(self) -> Tuple[List[str], str]:
        rows, err = self._read(self.dialect.distinct_tickers_sql())
        if err:
            return [], err
        return [r.get("company_ticker", "") for r in rows if r.get("company_ticker")], ERR_NONE

    def fetch_filing_ids_by_ticker(self, tickers: List[str]) -> Tuple[List[Dict[str, Any]], str]:
        if not tickers:
            return [], ERR_NONE
        sql = self.dialect.fetch_filing_ids_by_ticker_sql(len(tickers))
        return self._read(sql, list(tickers))

    def fetch_titles(
        self, ticker_set: Optional[List[str]], offset: int, page_size: int
    ) -> Tuple[List[Dict[str, Any]], str]:
        if ticker_set:
            sql = self.dialect.fetch_titles_sql(True, len(ticker_set))
            params: List[Any] = [*ticker_set, page_size, offset]
        else:
            sql = self.dialect.fetch_titles_sql(False)
            params = [page_size, offset]
        return self._read(sql, params)

    def fetch_coverage(self) -> Tuple[List[Dict[str, Any]], str]:
        return self._read(self.dialect.fetch_coverage_sql())


# Re-exported for tests and adapters that build statements directly.
__all__ = [
    "RelationalDriver",
    "RelationalSink",
    "FILING_COLUMNS",
    "DOCUMENT_COLUMNS",
    "COVERAGE_COLUMNS",
    "EDGE_KEYS",
]
