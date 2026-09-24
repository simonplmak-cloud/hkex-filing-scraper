"""PostgreSQL sink — adapter over the existing :mod:`hkex_scraper.db_postgres`.

The relational module stays the single implementation of the PostgreSQL schema,
pooling, ``jsonb`` handling, and parameterised upserts. This adapter only maps
that surface onto the uniform :class:`~hkex_scraper.sinks.base.Sink` contract.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .. import db_postgres
from .base import FilingQuery, Sink, SinkCapabilities


class PostgresSink(Sink):
    id = "postgres"
    capabilities = SinkCapabilities(
        model="relational",
        native_upsert=True,
        reads=True,
        edges=True,
        json=True,
        arrays=True,
        transactions=True,
        snippets=True,
    )

    def available(self) -> bool:
        return db_postgres.postgres_available()

    def unavailable_reason(self) -> str:
        if not db_postgres.driver_installed():
            return (
                "PostgreSQL sink requires the psycopg driver "
                '(install with: pip install ".[postgres]")'
            )
        return (
            "PostgreSQL sink requires POSTGRES_DSN or "
            "POSTGRES_DATABASE/POSTGRES_USER connection details"
        )

    def ensure_schema(self) -> Tuple[bool, str]:
        ok, code = db_postgres.initialize_postgres_schema()
        if ok:
            # Optional; logs a warning and never blocks a usable schema.
            db_postgres.ensure_search_indexes()
        return ok, code

    def close(self) -> None:
        db_postgres.close_pool()

    def upsert_filings(self, records: List[Dict[str, Any]]) -> Tuple[int, str]:
        return db_postgres.upsert_filings(records)

    def upsert_document(self, filing_id: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
        return db_postgres.upsert_document(filing_id, payload)

    def mark_status(self, filing_id: str, status: str, reason: str = "") -> Tuple[bool, str]:
        return db_postgres.mark_document_status(filing_id, status, reason)

    def insert_coverage(self, chunk: Dict[str, Any]) -> Tuple[bool, str]:
        return db_postgres.insert_coverage(chunk)

    def upsert_edges(self, edges: List[Dict[str, Any]], kind: str) -> Tuple[int, str]:
        return db_postgres.upsert_edges(edges, kind)

    def read_filing_digests(self) -> Tuple[List[Dict[str, Any]], str]:
        return db_postgres.read_filing_digests()

    def count_filings(self) -> Tuple[int, str]:
        return db_postgres.count_filings()

    def count_matching(self, query: FilingQuery) -> Tuple[int, str]:
        return db_postgres.count_matching(query)

    def count_edges(self, kind: str) -> Tuple[int, str]:
        return db_postgres.count_edges(kind)

    def list_edges(
        self, kind: str, company_id: str, limit: int, offset: int
    ) -> Tuple[List[Dict[str, Any]], str]:
        return db_postgres.list_edges(kind, company_id, limit, offset)

    def count_pending_filings(self) -> Tuple[int, str]:
        return db_postgres.count_pending_filings()

    def fetch_pending_filings(self, limit: int) -> Tuple[List[Dict[str, Any]], str]:
        return db_postgres.fetch_pending_filings(limit)

    def distinct_company_tickers(self) -> Tuple[List[str], str]:
        return db_postgres.distinct_company_tickers()

    def fetch_filing_ids_by_ticker(self, tickers: List[str]) -> Tuple[List[Dict[str, Any]], str]:
        return db_postgres.fetch_filing_ids_by_ticker(tickers)

    def fetch_titles(
        self,
        ticker_set: Optional[List[str]],
        offset: int,
        page_size: int,
        title_query: str = "",
    ) -> Tuple[List[Dict[str, Any]], str]:
        return db_postgres.fetch_titles(ticker_set, offset, page_size, title_query)

    def fetch_filing_detail(self, filing_id: str) -> Tuple[Optional[Dict[str, Any]], str]:
        return db_postgres.fetch_filing_detail(filing_id)

    def search_filings(
        self, query: FilingQuery, offset: int, limit: int
    ) -> Tuple[List[Dict[str, Any]], str]:
        return db_postgres.search_filings(query, offset, limit)

    def search_documents(
        self, query: FilingQuery, offset: int, limit: int
    ) -> Tuple[List[Dict[str, Any]], str]:
        return db_postgres.search_documents(query, offset, limit)

    def aggregate_filings(
        self, group_by: str, query: FilingQuery
    ) -> Tuple[List[Dict[str, Any]], str]:
        return db_postgres.aggregate_filings(group_by, query)

    def list_companies(
        self, limit: int, offset: int, ticker: str = ""
    ) -> Tuple[List[Dict[str, Any]], str]:
        return db_postgres.list_companies(limit, offset, ticker)

    def fetch_coverage(self) -> Tuple[List[Dict[str, Any]], str]:
        return db_postgres.fetch_coverage()
