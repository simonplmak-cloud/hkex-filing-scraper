"""SQL dialect descriptors for the relational sinks.

MySQL/MariaDB/SQLite differ only in placeholder syntax, identifier quoting,
type names, the conflict clause, and how duplicates are suppressed on edge
inserts. Those differences are data expressed by a :class:`Dialect`; the shared
execution loop lives in :mod:`hkex_scraper.sinks.relational`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Sequence, Tuple

from .base import AGGREGATE_GROUPS, SEARCH_COLUMNS, FilingQuery

# Canonical column order (mirrors db_postgres).
FILING_COLUMNS: Tuple[str, ...] = (
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
)

DOCUMENT_COLUMNS: Tuple[str, ...] = (
    "document_size",
    "document_type",
    "document_hash",
    "document_sha256",
    "document_text",
    "document_text_len",
    "document_tables",
    "document_table_cnt",
    "document_status",
    "document_status_reason",
)

COVERAGE_COLUMNS: Tuple[str, ...] = (
    "chunk_from",
    "chunk_to",
    "api_count",
    "ingested_count",
    "unique_count",
    "run_id",
    "timestamp",
)
COVERAGE_KEY: Tuple[str, ...] = ("chunk_from", "chunk_to", "run_id")

# Read columns that may arrive as JSON *text* (SQLite/DuckDB/MySQL) or as a native
# list/dict (PostgreSQL). Normalised by :func:`decode_row` for read consumers.
JSON_READ_FIELDS: Tuple[str, ...] = ("referenced_tickers", "document_tables")


def decode_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of *row* with JSON-text columns parsed into real values.

    Native list/dict values (PostgreSQL ``jsonb``) are left untouched; empty strings
    become ``None`` so a consumer sees "absent" rather than a bogus empty value.
    """
    decoded = dict(row)
    for name in JSON_READ_FIELDS:
        value = decoded.get(name)
        if isinstance(value, (str, bytes)) and value:
            try:
                decoded[name] = json.loads(value)
            except (ValueError, TypeError):
                pass  # leave the raw text rather than fail the read
        elif value == "":
            decoded[name] = None
    return decoded


EDGE_TABLES = {"has_filing": "has_filing", "references_filing": "references_filing"}
EDGE_COLUMNS = {
    "has_filing": ("company_id", "filing_id"),
    "references_filing": ("filing_id", "company_id", "source"),
}
EDGE_KEYS = {
    "has_filing": ("company_id", "filing_id"),
    "references_filing": ("filing_id", "company_id"),
}


@dataclass(frozen=True)
class Dialect:
    """Describes one SQL dialect's syntax and types.

    ``_ef_indexes`` are indexes on ``exchange_filing``; ``_extra_indexes`` are
    ``(name, table, columns)`` indexes for the coverage/edge tables.
    """

    id: str
    ph: str = "%s"
    quote_char: str = '"'
    text_type: str = "text"
    long_text_type: str = "text"
    int_type: str = "integer"
    bigint_type: str = "bigint"
    bool_type: str = "boolean"
    datetime_type: str = "timestamptz"
    date_type: str = "date"
    now_default: str = "now()"
    json_type: str = "text"
    array_type: str = "text"
    datetime_as_text: bool = False
    edge_conflict: str = "do-nothing"  # do-nothing | insert-ignore
    supports_returning: bool = False
    _ef_indexes: Tuple[Tuple[str, Tuple[str, ...]], ...] = field(default_factory=tuple)
    _extra_indexes: Tuple[Tuple[str, str, Tuple[str, ...]], ...] = field(default_factory=tuple)

    # -- quoting -----------------------------------------------------------
    def q(self, ident: str) -> str:
        c = self.quote_char
        return f"{c}{ident}{c}"

    def columns(self, cols: Sequence[str]) -> str:
        return ", ".join(self.q(c) for c in cols)

    # -- value adaptation --------------------------------------------------
    def encode(self, value: Any) -> Any:
        """Adapt a canonical Python value for this dialect's driver."""
        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False)
        if self.datetime_as_text and isinstance(value, (datetime, date)):
            return value.isoformat()
        return value

    def encode_row(self, row: Sequence[Any]) -> tuple:
        return tuple(self.encode(v) for v in row)

    # -- conflict clauses --------------------------------------------------
    def _upsert_suffix(self, key: Sequence[str], cols: Sequence[str]) -> str:
        updates = ", ".join(f"{self.q(c)} = EXCLUDED.{self.q(c)}" for c in cols if c not in key)
        return f"ON CONFLICT ({self.columns(key)}) DO UPDATE SET {updates}"

    def _duplicate_key_suffix(self, key: Sequence[str], cols: Sequence[str]) -> str:
        updates = ", ".join(f"{self.q(c)} = VALUES({self.q(c)})" for c in cols if c not in key)
        return f"ON DUPLICATE KEY UPDATE {updates}"

    # -- DDL ---------------------------------------------------------------
    def _column_ddl(self) -> str:
        return (
            f"{self.q('filing_id')} {self.text_type} PRIMARY KEY, "
            f"{self.q('company_ticker')} {self.text_type} NOT NULL, "
            f"{self.q('stock_code')} {self.text_type} NOT NULL, "
            f"{self.q('stock_name')} {self.text_type}, "
            f"{self.q('exchange')} {self.text_type} NOT NULL, "
            f"{self.q('filing_type')} {self.text_type} NOT NULL, "
            f"{self.q('filing_subtype')} {self.text_type}, "
            f"{self.q('filing_category')} {self.text_type}, "
            f"{self.q('title')} {self.long_text_type}, "
            f"{self.q('filing_date')} {self.datetime_type}, "
            f"{self.q('document_url')} {self.long_text_type}, "
            f"{self.q('referenced_tickers')} {self.array_type}, "
            f"{self.q('source')} {self.text_type} NOT NULL, "
            f"{self.q('updated_at')} {self.datetime_type} NOT NULL, "
            f"{self.q('document_size')} {self.bigint_type}, "
            f"{self.q('document_type')} {self.text_type}, "
            f"{self.q('document_hash')} {self.text_type}, "
            f"{self.q('document_sha256')} {self.text_type}, "
            f"{self.q('document_text')} {self.long_text_type}, "
            f"{self.q('document_text_len')} {self.int_type}, "
            f"{self.q('document_tables')} {self.json_type}, "
            f"{self.q('document_table_cnt')} {self.int_type}, "
            f"{self.q('document_status')} {self.text_type}, "
            f"{self.q('document_status_reason')} {self.text_type}"
        )

    def _coverage_ddl(self, id_clause: str) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {self.q('scrape_coverage')} ("
            f"{id_clause}, "
            f"{self.q('chunk_from')} {self.datetime_type} NOT NULL, "
            f"{self.q('chunk_to')} {self.datetime_type} NOT NULL, "
            f"{self.q('api_count')} {self.int_type} NOT NULL, "
            f"{self.q('ingested_count')} {self.int_type} NOT NULL, "
            f"{self.q('unique_count')} {self.int_type} NOT NULL, "
            f"{self.q('run_id')} {self.text_type} NOT NULL, "
            f"{self.q('timestamp')} {self.datetime_type} NOT NULL, "
            f"UNIQUE ({self.columns(COVERAGE_KEY)}){self._coverage_suffix()}){self._table_suffix()}"
        )

    def _coverage_suffix(self) -> str:
        return ""

    def _table_suffix(self) -> str:
        return ""

    def _edge_ddl(self, kind: str) -> str:
        table = EDGE_TABLES[kind]
        if kind == "has_filing":
            body = (
                f"{self.q('company_id')} {self.text_type} NOT NULL, "
                f"{self.q('filing_id')} {self.text_type} NOT NULL, "
                f"{self.q('created_at')} {self.datetime_type} NOT NULL "
                f"DEFAULT {self.now_default}, "
                f"PRIMARY KEY ({self.columns(EDGE_KEYS[kind])})"
            )
        else:
            body = (
                f"{self.q('filing_id')} {self.text_type} NOT NULL, "
                f"{self.q('company_id')} {self.text_type} NOT NULL, "
                f"{self.q('source')} {self.text_type}, "
                f"{self.q('created_at')} {self.datetime_type} NOT NULL "
                f"DEFAULT {self.now_default}, "
                f"PRIMARY KEY ({self.columns(EDGE_KEYS[kind])})"
            )
        return f"CREATE TABLE IF NOT EXISTS {self.q(table)} ({body}){self._table_suffix()}"

    def ddl(self) -> List[str]:
        """Return idempotent ``CREATE`` statements for this dialect."""
        raise NotImplementedError

    # -- write statements --------------------------------------------------
    def upsert_filings_sql(self) -> str:
        placeholders = ", ".join([self.ph] * len(FILING_COLUMNS))
        return (
            f"INSERT INTO {self.q('exchange_filing')} ({self.columns(FILING_COLUMNS)}) "
            f"VALUES ({placeholders}) "
            f"{self._upsert_suffix(('filing_id',), FILING_COLUMNS)}"
        )

    def upsert_document_sql(self) -> str:
        sets = ", ".join(f"{self.q(c)} = {self.ph}" for c in DOCUMENT_COLUMNS)
        return (
            f"UPDATE {self.q('exchange_filing')} SET {sets}, "
            f"{self.q('updated_at')} = {self.ph} WHERE {self.q('filing_id')} = {self.ph}"
        )

    def mark_status_sql(self) -> str:
        return (
            f"UPDATE {self.q('exchange_filing')} SET "
            f"{self.q('document_status')} = {self.ph}, "
            f"{self.q('document_status_reason')} = {self.ph}, "
            f"{self.q('updated_at')} = {self.ph} WHERE {self.q('filing_id')} = {self.ph}"
        )

    def insert_coverage_sql(self) -> str:
        placeholders = ", ".join([self.ph] * len(COVERAGE_COLUMNS))
        return (
            f"INSERT INTO {self.q('scrape_coverage')} ({self.columns(COVERAGE_COLUMNS)}) "
            f"VALUES ({placeholders}) "
            f"{self._upsert_suffix(COVERAGE_KEY, COVERAGE_COLUMNS)}"
        )

    def upsert_edge_sql(self, kind: str) -> str:
        table = EDGE_TABLES[kind]
        cols = EDGE_COLUMNS[kind]
        placeholders = ", ".join([self.ph] * len(cols))
        if self.edge_conflict == "insert-ignore":
            return (
                f"INSERT IGNORE INTO {self.q(table)} ({self.columns(cols)}) VALUES ({placeholders})"
            )
        return (
            f"INSERT INTO {self.q(table)} ({self.columns(cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT ({self.columns(EDGE_KEYS[kind])}) DO NOTHING"
        )

    # -- read statements ---------------------------------------------------
    def count_filings_sql(self) -> str:
        return f"SELECT count(*) FROM {self.q('exchange_filing')}"

    def count_edges_sql(self, kind: str) -> str:
        return f"SELECT count(*) FROM {self.q(EDGE_TABLES[kind])}"

    def count_pending_sql(self) -> str:
        return (
            f"SELECT count(*) FROM {self.q('exchange_filing')} "
            f"WHERE {self.q('document_status')} IS NULL "
            f"AND {self.q('document_url')} IS NOT NULL AND {self.q('document_url')} <> ''"
        )

    def select_digests_sql(self) -> str:
        """Every filing id with its integrity hash, ordered for a stable diff."""
        return (
            f"SELECT {self.q('filing_id')}, {self.q('document_sha256')} "
            f"FROM {self.q('exchange_filing')} ORDER BY {self.q('filing_id')}"
        )

    def fetch_pending_sql(self) -> str:
        return (
            f"SELECT {self.q('filing_id')}, {self.q('document_url')}, {self.q('filing_date')} "
            f"FROM {self.q('exchange_filing')} "
            f"WHERE {self.q('document_status')} IS NULL "
            f"AND {self.q('document_url')} IS NOT NULL AND {self.q('document_url')} <> '' "
            f"ORDER BY {self.q('filing_date')} DESC LIMIT {self.ph}"
        )

    def in_clause(self, n: int) -> str:
        return "(" + ", ".join([self.ph] * n) + ")"

    def fetch_filing_ids_by_ticker_sql(self, n: int) -> str:
        return (
            f"SELECT {self.q('company_ticker')}, {self.q('filing_id')} "
            f"FROM {self.q('exchange_filing')} "
            f"WHERE {self.q('company_ticker')} IN {self.in_clause(n)}"
        )

    def distinct_tickers_sql(self) -> str:
        return (
            f"SELECT DISTINCT {self.q('company_ticker')} FROM {self.q('exchange_filing')} "
            f"WHERE {self.q('company_ticker')} IS NOT NULL"
        )

    def fetch_titles_sql(self, with_tickers: bool, n: int = 1, with_query: bool = False) -> str:
        cols = (
            f"{self.q('filing_id')}, {self.q('title')}, "
            f"{self.q('stock_code')}, {self.q('company_ticker')}"
        )
        clauses = []
        if with_tickers:
            clauses.append(f"{self.q('company_ticker')} IN {self.in_clause(n)}")
        clauses.append(f"{self.q('title')} IS NOT NULL")
        if with_query:
            # Dialect-agnostic case-insensitive substring match.
            clauses.append(f"LOWER({self.q('title')}) LIKE LOWER({self.ph})")
        where = " AND ".join(clauses)
        return (
            f"SELECT {cols} FROM {self.q('exchange_filing')} WHERE {where} "
            f"ORDER BY {self.q('filing_id')} ASC LIMIT {self.ph} OFFSET {self.ph}"
        )

    def fetch_filing_detail_sql(self) -> str:
        """One filing's metadata plus its extracted document columns."""
        cols = self.columns((*FILING_COLUMNS, *DOCUMENT_COLUMNS))
        return (
            f"SELECT {cols} FROM {self.q('exchange_filing')} "
            f"WHERE {self.q('filing_id')} = {self.ph} LIMIT 1"
        )

    def fetch_coverage_sql(self) -> str:
        return (
            f"SELECT {self.columns(COVERAGE_COLUMNS)} FROM {self.q('scrape_coverage')} "
            f"ORDER BY {self.q('chunk_from')} DESC"
        )

    def list_edges_sql(self, kind: str) -> str:
        """Edge rows for one company, keyed by ``company_id`` (``LIMIT``/``OFFSET``)."""
        table = EDGE_TABLES[kind]
        cols = self.columns(EDGE_COLUMNS[kind])
        return (
            f"SELECT {cols} FROM {self.q(table)} WHERE {self.q('company_id')} = {self.ph} "
            f"ORDER BY {self.q('filing_id')} ASC LIMIT {self.ph} OFFSET {self.ph}"
        )

    # -- composable search -------------------------------------------------
    def cast_text(self, expr: str) -> str:
        """Cast *expr* to text for a portable ``LIKE`` on a JSON/array column."""
        return f"CAST({expr} AS TEXT)"

    def date_only(self, expr: str) -> str:
        """Reduce a date/datetime column to a calendar date for inclusive range filters."""
        return f"CAST({expr} AS DATE)"

    def snippet_sql(self) -> str:
        """A ~320-char window around the first match of this dialect's text placeholder."""
        col = self.q("document_text")
        return f"substr({col}, GREATEST(1, STRPOS(LOWER({col}), LOWER({self.ph})) - 80), 320)"

    def _search_clauses(self, query: FilingQuery) -> Tuple[List[str], List[Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        placeholders = lambda n: ", ".join([self.ph] * n)  # noqa: E731
        if query.tickers:
            clauses.append(f"{self.q('company_ticker')} IN ({placeholders(len(query.tickers))})")
            params.extend(query.tickers)
        if query.stock_codes:
            clauses.append(f"{self.q('stock_code')} IN ({placeholders(len(query.stock_codes))})")
            params.extend(query.stock_codes)
        if query.title_query:
            clauses.append(f"LOWER({self.q('title')}) LIKE LOWER({self.ph})")
            params.append(f"%{query.title_query}%")
        if query.text_query:
            clauses.append(f"LOWER({self.q('document_text')}) LIKE LOWER({self.ph})")
            params.append(f"%{query.text_query}%")
        if query.filing_types:
            clauses.append(f"{self.q('filing_type')} IN ({placeholders(len(query.filing_types))})")
            params.extend(query.filing_types)
        if query.filing_categories:
            clauses.append(
                f"{self.q('filing_category')} IN ({placeholders(len(query.filing_categories))})"
            )
            params.extend(query.filing_categories)
        if query.document_status:
            real = [s for s in query.document_status if s and s != "unprocessed"]
            parts: List[str] = []
            if real:
                parts.append(f"{self.q('document_status')} IN ({placeholders(len(real))})")
                params.extend(real)
            if any(s == "unprocessed" for s in query.document_status):
                parts.append(f"{self.q('document_status')} IS NULL")
            if parts:
                clauses.append("(" + " OR ".join(parts) + ")")
        if query.exchange:
            clauses.append(f"{self.q('exchange')} = {self.ph}")
            params.append(query.exchange)
        if query.source:
            clauses.append(f"{self.q('source')} = {self.ph}")
            params.append(query.source)
        if query.document_type:
            clauses.append(f"{self.q('document_type')} = {self.ph}")
            params.append(query.document_type)
        if query.referenced_ticker:
            ref = self.cast_text(self.q("referenced_tickers"))
            clauses.append(f"LOWER({ref}) LIKE LOWER({self.ph})")
            params.append(f"%{query.referenced_ticker}%")
        if query.date_from:
            clauses.append(f"{self.date_only(self.q('filing_date'))} >= {self.ph}")
            params.append(query.date_from)
        if query.date_to:
            clauses.append(f"{self.date_only(self.q('filing_date'))} <= {self.ph}")
            params.append(query.date_to)
        return clauses, params

    def _search_where(self, query: FilingQuery) -> Tuple[str, List[Any]]:
        clauses, params = self._search_clauses(query)
        return (" AND ".join(clauses) if clauses else "1 = 1"), params

    def _search_order(self, order_by: str) -> str:
        date = self.q("filing_date")
        fid = self.q("filing_id")
        if order_by == "filing_date_asc":
            return f"({date} IS NULL) ASC, {date} ASC, {fid} ASC"
        if order_by == "title_asc":
            return f"{self.q('title')} ASC, {fid} ASC"
        if order_by == "filing_id_asc":
            return f"{fid} ASC"
        return f"({date} IS NULL) ASC, {date} DESC, {fid} ASC"

    def search_filings_sql(self, query: FilingQuery, limit: int, offset: int) -> Tuple[str, list]:
        cols = self.columns(SEARCH_COLUMNS)
        where, params = self._search_where(query)
        order = self._search_order(query.order_by)
        sql = (
            f"SELECT {cols} FROM {self.q('exchange_filing')} WHERE {where} "
            f"ORDER BY {order} LIMIT {self.ph} OFFSET {self.ph}"
        )
        return sql, [*params, limit, offset]

    def search_documents_sql(self, query: FilingQuery, limit: int, offset: int) -> Tuple[str, list]:
        cols = self.columns(SEARCH_COLUMNS)
        where, params = self._search_where(query)
        order = self._search_order(query.order_by)
        snippet = self.snippet_sql()
        sql = (
            f"SELECT {cols}, {snippet} AS {self.q('snippet')} "
            f"FROM {self.q('exchange_filing')} WHERE {where} "
            f"ORDER BY {order} LIMIT {self.ph} OFFSET {self.ph}"
        )
        # The snippet placeholder comes first in the SELECT list.
        return sql, [f"%{query.text_query}%", *params, limit, offset]

    def aggregate_filings_sql(self, group_by: str, query: FilingQuery) -> Tuple[str, list]:
        if group_by not in AGGREGATE_GROUPS:
            raise ValueError(f"unsupported group_by '{group_by}'")
        col = self.q(group_by)
        where, params = self._search_where(query)
        key, count = self.q("key"), self.q("count")
        sql = (
            f"SELECT {col} AS {key}, count(*) AS {count} FROM {self.q('exchange_filing')} "
            f"WHERE {where} GROUP BY {col} ORDER BY {count} DESC, {key} ASC LIMIT 200"
        )
        return sql, params

    def count_matching_sql(self, query: FilingQuery) -> Tuple[str, list]:
        """Count filings matching *query* without returning rows."""
        where, params = self._search_where(query)
        sql = f"SELECT count(*) FROM {self.q('exchange_filing')} WHERE {where}"
        return sql, params

    def list_companies_sql(self, limit: int, offset: int) -> Tuple[str, list]:
        ticker = self.q("company_ticker")
        sql = (
            f"SELECT {ticker} AS {self.q('company_ticker')}, "
            f"MAX({self.q('stock_name')}) AS {self.q('stock_name')}, "
            f"count(*) AS {self.q('filing_count')} FROM {self.q('exchange_filing')} "
            f"WHERE {ticker} IS NOT NULL AND {ticker} <> '' GROUP BY {ticker} "
            f"ORDER BY {self.q('filing_count')} DESC, {ticker} ASC LIMIT {self.ph} OFFSET {self.ph}"
        )
        return sql, [limit, offset]


class PostgresDialect(Dialect):
    """PostgreSQL dialect (reference/assertions; the live sink uses db_postgres)."""

    def __init__(self) -> None:
        super().__init__(
            id="postgres",
            ph="%s",
            quote_char='"',
            text_type="text",
            long_text_type="text",
            int_type="integer",
            bigint_type="bigint",
            bool_type="boolean",
            datetime_type="timestamptz",
            date_type="date",
            json_type="jsonb",
            array_type="text[]",
            edge_conflict="do-nothing",
            supports_returning=True,
        )

    def ddl(self) -> List[str]:
        return [
            f"CREATE TABLE IF NOT EXISTS {self.q('exchange_filing')} ({self._column_ddl()})",
            self._coverage_ddl(f"{self.q('id')} bigserial PRIMARY KEY"),
            self._edge_ddl("has_filing"),
            self._edge_ddl("references_filing"),
        ]


class MySQLDialect(Dialect):
    """MySQL/MariaDB dialect. Indexes are declared inline for idempotency."""

    def __init__(self, id: str = "mysql") -> None:  # noqa: A002 - mirrors the base field
        super().__init__(
            id=id,
            ph="%s",
            quote_char="`",
            text_type="varchar(255)",
            long_text_type="longtext",
            int_type="int",
            bigint_type="bigint",
            bool_type="tinyint(1)",
            datetime_type="datetime(6)",
            date_type="date",
            now_default="CURRENT_TIMESTAMP(6)",
            json_type="json",
            array_type="json",
            edge_conflict="insert-ignore",
            supports_returning=False,
            _ef_indexes=(
                ("idx_my_ef_ticker", ("company_ticker",)),
                ("idx_my_ef_stockcode", ("stock_code",)),
                ("idx_my_ef_date", ("filing_date",)),
                ("idx_my_ef_type", ("filing_type",)),
                ("idx_my_ef_source", ("source",)),
                ("idx_my_ef_docstatus", ("document_status",)),
                ("idx_my_ef_category", ("filing_category",)),
            ),
            _extra_indexes=(
                ("idx_my_cov_run", "scrape_coverage", ("run_id",)),
                ("idx_my_hf_filing", "has_filing", ("filing_id",)),
                ("idx_my_rf_company", "references_filing", ("company_id",)),
            ),
        )

    def _table_suffix(self) -> str:
        return " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

    def _ef_index_sql(self) -> str:
        clauses = []
        for name, cols in self._ef_indexes:
            clauses.append(f"KEY {self.q(name)} ({self.columns(cols)})")
        return (", " + ", ".join(clauses)) if clauses else ""

    def _extra_index_sql(self, table: str) -> str:
        clauses = [
            f"KEY {self.q(name)} ({self.columns(cols)})"
            for name, tbl, cols in self._extra_indexes
            if tbl == table
        ]
        return (", " + ", ".join(clauses)) if clauses else ""

    def _coverage_suffix(self) -> str:
        return self._extra_index_sql("scrape_coverage")

    def ddl(self) -> List[str]:
        ef = (
            f"CREATE TABLE IF NOT EXISTS {self.q('exchange_filing')} ("
            f"{self._column_ddl()}{self._ef_index_sql()}){self._table_suffix()}"
        )
        coverage = self._coverage_ddl(f"{self.q('id')} bigint NOT NULL AUTO_INCREMENT PRIMARY KEY")
        # ``_edge_ddl`` appends the per-table indexes via ``_edge_index_suffix``.
        return [ef, coverage, self._edge_ddl("has_filing"), self._edge_ddl("references_filing")]

    def _edge_ddl(self, kind: str) -> str:
        table = EDGE_TABLES[kind]
        if kind == "has_filing":
            body = (
                f"{self.q('company_id')} {self.text_type} NOT NULL, "
                f"{self.q('filing_id')} {self.text_type} NOT NULL, "
                f"{self.q('created_at')} {self.datetime_type} NOT NULL "
                f"DEFAULT {self.now_default}, "
                f"PRIMARY KEY ({self.columns(EDGE_KEYS[kind])})"
            )
        else:
            body = (
                f"{self.q('filing_id')} {self.text_type} NOT NULL, "
                f"{self.q('company_id')} {self.text_type} NOT NULL, "
                f"{self.q('source')} {self.text_type}, "
                f"{self.q('created_at')} {self.datetime_type} NOT NULL "
                f"DEFAULT {self.now_default}, "
                f"PRIMARY KEY ({self.columns(EDGE_KEYS[kind])})"
            )
        return (
            f"CREATE TABLE IF NOT EXISTS {self.q(table)} ("
            f"{body}{self._extra_index_sql(table)}){self._table_suffix()}"
        )

    def _upsert_suffix(self, key: Sequence[str], cols: Sequence[str]) -> str:
        return self._duplicate_key_suffix(key, cols)

    def cast_text(self, expr: str) -> str:
        return f"CAST({expr} AS CHAR)"

    def date_only(self, expr: str) -> str:
        return f"DATE({expr})"

    def snippet_sql(self) -> str:
        col = self.q("document_text")
        return f"SUBSTRING({col}, GREATEST(1, LOCATE(LOWER({self.ph}), LOWER({col})) - 80), 320)"


class SQLiteDialect(Dialect):
    """SQLite dialect. Datetimes and JSON are stored as ISO-8601 / JSON text."""

    def __init__(self) -> None:
        super().__init__(
            id="sqlite",
            ph="?",
            quote_char='"',
            text_type="text",
            long_text_type="text",
            int_type="integer",
            bigint_type="integer",
            bool_type="integer",
            datetime_type="text",
            date_type="text",
            now_default="CURRENT_TIMESTAMP",
            json_type="text",
            array_type="text",
            datetime_as_text=True,
            edge_conflict="do-nothing",
            supports_returning=False,
            _ef_indexes=(
                ("idx_sq_ef_ticker", ("company_ticker",)),
                ("idx_sq_ef_stockcode", ("stock_code",)),
                ("idx_sq_ef_date", ("filing_date",)),
                ("idx_sq_ef_type", ("filing_type",)),
                ("idx_sq_ef_source", ("source",)),
                ("idx_sq_ef_docstatus", ("document_status",)),
                ("idx_sq_ef_category", ("filing_category",)),
            ),
            _extra_indexes=(
                ("idx_sq_cov_run", "scrape_coverage", ("run_id",)),
                ("idx_sq_hf_filing", "has_filing", ("filing_id",)),
                ("idx_sq_rf_company", "references_filing", ("company_id",)),
            ),
        )

    def ddl(self) -> List[str]:
        statements = [
            f"CREATE TABLE IF NOT EXISTS {self.q('exchange_filing')} ({self._column_ddl()})",
            self._coverage_ddl(f"{self.q('id')} integer PRIMARY KEY AUTOINCREMENT"),
            self._edge_ddl("has_filing"),
            self._edge_ddl("references_filing"),
        ]
        for name, cols in self._ef_indexes:
            statements.append(
                f"CREATE INDEX IF NOT EXISTS {self.q(name)} ON {self.q('exchange_filing')} "
                f"({self.columns(cols)})"
            )
        for name, table, cols in self._extra_indexes:
            statements.append(
                f"CREATE INDEX IF NOT EXISTS {self.q(name)} ON {self.q(table)} "
                f"({self.columns(cols)})"
            )
        return statements

    def snippet_sql(self) -> str:
        col = self.q("document_text")
        return f"substr({col}, max(1, instr(lower({col}), lower({self.ph})) - 80), 320)"

    def date_only(self, expr: str) -> str:
        return f"substr({expr}, 1, 10)"


class DuckDBDialect(Dialect):
    """DuckDB dialect. In-process analytical SQL; JSON stored as JSON text."""

    def __init__(self) -> None:
        super().__init__(
            id="duckdb",
            ph="?",
            quote_char='"',
            text_type="VARCHAR",
            long_text_type="VARCHAR",
            int_type="INTEGER",
            bigint_type="BIGINT",
            bool_type="BOOLEAN",
            datetime_type="TIMESTAMP",
            date_type="DATE",
            json_type="JSON",
            array_type="JSON",
            edge_conflict="do-nothing",
            supports_returning=True,
        )

    def ddl(self) -> List[str]:
        # Primary keys only; DuckDB does not need (and is faster without) mirror indexes.
        coverage = (
            f"CREATE TABLE IF NOT EXISTS {self.q('scrape_coverage')} ("
            f"{self.q('chunk_from')} {self.datetime_type} NOT NULL, "
            f"{self.q('chunk_to')} {self.datetime_type} NOT NULL, "
            f"{self.q('api_count')} {self.int_type} NOT NULL, "
            f"{self.q('ingested_count')} {self.int_type} NOT NULL, "
            f"{self.q('unique_count')} {self.int_type} NOT NULL, "
            f"{self.q('run_id')} {self.text_type} NOT NULL, "
            f"{self.q('timestamp')} {self.datetime_type} NOT NULL, "
            f"PRIMARY KEY ({self.columns(COVERAGE_KEY)}))"
        )
        return [
            f"CREATE TABLE IF NOT EXISTS {self.q('exchange_filing')} ({self._column_ddl()})",
            coverage,
            self._edge_ddl("has_filing"),
            self._edge_ddl("references_filing"),
        ]

    # ``RETURNING`` lets the shared engine count affected rows (DuckDB has no rowcount).
    def upsert_document_sql(self) -> str:
        return super().upsert_document_sql() + " RETURNING 1"

    def mark_status_sql(self) -> str:
        return super().mark_status_sql() + " RETURNING 1"

    def upsert_edge_sql(self, kind: str) -> str:
        return super().upsert_edge_sql(kind) + " RETURNING 1"
