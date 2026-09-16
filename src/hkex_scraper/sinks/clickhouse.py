"""ClickHouse sink — optional ``clickhouse-connect`` driver, columnar model.

ClickHouse has no row-level upsert. Tables use ``ReplacingMergeTree`` (rows with
the same sort key collapse on merge; reads use ``FINAL``), and metadata/document
writes are **read-merge-reinsert** so a metadata write preserves the document
columns and vice versa. The sink therefore declares ``native_upsert=False``.

``documentTables`` is stored as a JSON string (nested objects are not a ClickHouse
column type); ``referencedTickers`` uses ``Array(String)``.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .. import config
from ..utils import ticker_to_record_id
from .base import (
    EDGE_KINDS,
    ERR_NONE,
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

try:  # pragma: no cover - exercised via monkeypatching in tests
    import clickhouse_connect  # type: ignore

    _CLICKHOUSE_AVAILABLE = True
except Exception:  # pragma: no cover - depends on environment
    clickhouse_connect = None  # type: ignore
    _CLICKHOUSE_AVAILABLE = False

FILING_TABLE = "exchange_filing"
COVERAGE_TABLE = "scrape_coverage"
EDGE_TABLES = {"has_filing": "has_filing", "references_filing": "references_filing"}

DT = "DateTime64(3, 'UTC')"

_METADATA_COLUMNS = [
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
]

_DOCUMENT_COLUMNS = [
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
]

_FILING_COLUMNS = _METADATA_COLUMNS + _DOCUMENT_COLUMNS

_DDL = [
    (
        f"CREATE TABLE IF NOT EXISTS {FILING_TABLE} ("
        " filing_id String,"
        " company_ticker String,"
        " stock_code String,"
        " stock_name Nullable(String),"
        " exchange String,"
        " filing_type String,"
        " filing_subtype Nullable(String),"
        " filing_category Nullable(String),"
        " title Nullable(String),"
        " filing_date Nullable(Date),"
        " document_url Nullable(String),"
        " referenced_tickers Array(String),"
        " source String,"
        f" updated_at {DT},"
        " document_size Nullable(Int64),"
        " document_type Nullable(String),"
        " document_hash Nullable(String),"
        " document_sha256 Nullable(String),"
        " document_text Nullable(String),"
        " document_text_len Nullable(Int32),"
        " document_tables Nullable(String),"
        " document_table_cnt Nullable(Int32),"
        " document_status Nullable(String),"
        " document_status_reason Nullable(String)"
        f") ENGINE = ReplacingMergeTree(updated_at) ORDER BY filing_id"
    ),
    (
        f"CREATE TABLE IF NOT EXISTS {COVERAGE_TABLE} ("
        f" chunk_from {DT},"
        f" chunk_to {DT},"
        " api_count Int32,"
        " ingested_count Int32,"
        " unique_count Int32,"
        " run_id String,"
        f" timestamp {DT}"
        ") ENGINE = ReplacingMergeTree(timestamp) "
        "ORDER BY (chunk_from, chunk_to, run_id)"
    ),
    (
        f"CREATE TABLE IF NOT EXISTS {EDGE_TABLES['has_filing']} ("
        " company_id String,"
        " filing_id String,"
        f" created_at {DT}"
        ") ENGINE = ReplacingMergeTree(created_at) ORDER BY (company_id, filing_id)"
    ),
    (
        f"CREATE TABLE IF NOT EXISTS {EDGE_TABLES['references_filing']} ("
        " filing_id String,"
        " company_id String,"
        " source Nullable(String),"
        f" created_at {DT}"
        ") ENGINE = ReplacingMergeTree(created_at) ORDER BY (filing_id, company_id)"
    ),
]


class ClickHouseSink(Sink):
    id = "clickhouse"
    capabilities = SinkCapabilities(
        model="columnar",
        native_upsert=False,
        reads=True,
        edges=True,
        json=False,
        arrays=True,
        transactions=False,
        bulk=True,
    )

    def __init__(self) -> None:
        self._client = None

    # -- lifecycle ---------------------------------------------------------
    def driver_installed(self) -> bool:
        return _CLICKHOUSE_AVAILABLE

    def configured(self) -> bool:
        return bool(config.CLICKHOUSE_HOST and config.CLICKHOUSE_DATABASE)

    def available(self) -> bool:
        return self.driver_installed() and self.configured()

    def unavailable_reason(self) -> str:
        if not _CLICKHOUSE_AVAILABLE:
            return (
                "ClickHouse sink requires clickhouse-connect "
                '(install with: pip install ".[clickhouse]")'
            )
        if not config.CLICKHOUSE_HOST:
            return "ClickHouse sink requires CLICKHOUSE_HOST"
        if not config.CLICKHOUSE_DATABASE:
            return "ClickHouse sink requires CLICKHOUSE_DATABASE"
        return "ClickHouse sink is unavailable"

    def _get_client(self) -> Tuple[Any, str]:
        if not _CLICKHOUSE_AVAILABLE or clickhouse_connect is None:
            return None, code(self.id, SUFFIX_DRIVER_MISSING)
        if not self.configured():
            return None, code(self.id, SUFFIX_DSN_MISSING)
        if self._client is None:
            try:
                self._client = clickhouse_connect.get_client(
                    host=config.CLICKHOUSE_HOST,
                    port=int(config.CLICKHOUSE_PORT or "8123"),
                    username=config.CLICKHOUSE_USER or "default",
                    password=config.CLICKHOUSE_PASSWORD or "",
                    database=config.CLICKHOUSE_DATABASE,
                )
            except Exception as exc:  # noqa: BLE001
                return None, redact(str(exc)) or code(self.id, SUFFIX_WRITE_ERROR)
        return self._client, ERR_NONE

    def ensure_schema(self) -> Tuple[bool, str]:
        client, err = self._get_client()
        if err:
            return False, err
        for statement in _DDL:
            try:
                client.command(statement)
            except Exception as exc:  # noqa: BLE001
                return False, redact(str(exc)) or code(self.id, SUFFIX_SCHEMA_ERROR)
        return True, ERR_NONE

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:  # pragma: no cover - best effort
                pass
            self._client = None

    # -- query helpers -----------------------------------------------------
    def _query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Tuple[list, str]:
        client, err = self._get_client()
        if err:
            return [], err
        try:
            result = client.query(sql, parameters=params or {})
            names = list(result.column_names)
            return [dict(zip(names, row)) for row in result.result_rows], ERR_NONE
        except Exception as exc:  # noqa: BLE001
            return [], redact(str(exc)) or code(self.id, SUFFIX_WRITE_ERROR)

    def _scalar(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Tuple[int, str]:
        rows, err = self._query(sql, params)
        if err:
            return 0, err
        if not rows:
            return 0, ERR_NONE
        value = next(iter(rows[0].values()), 0)
        try:
            return int(value or 0), ERR_NONE
        except (TypeError, ValueError):
            return 0, code(self.id, SUFFIX_WRITE_ERROR)

    def _insert(self, table: str, rows: List[list], columns: List[str]) -> Tuple[bool, str]:
        if not rows:
            return True, ERR_NONE
        client, err = self._get_client()
        if err:
            return False, err
        try:
            client.insert(table, rows, column_names=columns)
            return True, ERR_NONE
        except Exception as exc:  # noqa: BLE001
            return False, redact(str(exc)) or code(self.id, SUFFIX_WRITE_ERROR)

    # -- value conversion --------------------------------------------------
    @classmethod
    def _filing_values(cls, row: Dict[str, Any]) -> list:
        values: list = []
        for column in _FILING_COLUMNS:
            value = row.get(column)
            if column == "filing_date" and isinstance(value, datetime):
                value = value.date()
            elif column == "referenced_tickers":
                value = list(value) if isinstance(value, list) else []
            elif column == "document_tables":
                if isinstance(value, (list, dict)):
                    value = json.dumps(value, ensure_ascii=False)
                elif value is None:
                    value = "[]"
            elif column == "updated_at":
                value = value or datetime.now(timezone.utc)
            values.append(value)
        return values

    def _fetch_filing(self, filing_id: str) -> Tuple[Optional[Dict[str, Any]], str]:
        rows, err = self._query(
            f"SELECT {', '.join(_FILING_COLUMNS)} FROM {FILING_TABLE} FINAL "
            "WHERE filing_id = {fid:String} LIMIT 1",
            {"fid": filing_id},
        )
        if err:
            return None, err
        return (rows[0] if rows else None), ERR_NONE

    # -- writes ------------------------------------------------------------
    def upsert_filings(self, records: List[Dict[str, Any]]) -> Tuple[int, str]:
        if not records:
            return 0, ERR_NONE
        ids = [record["filing_id"] for record in records]
        existing_rows, err = self._query(
            f"SELECT filing_id, {', '.join(_DOCUMENT_COLUMNS)} FROM {FILING_TABLE} FINAL "
            "WHERE filing_id IN {ids:Array(String)}",
            {"ids": ids},
        )
        if err:
            return 0, err
        existing = {row["filing_id"]: row for row in existing_rows}

        rows = []
        for record in records:
            merged = dict(existing.get(record["filing_id"], {}))
            merged.update(record)
            rows.append(self._filing_values(merged))
        ok, err = self._insert(FILING_TABLE, rows, _FILING_COLUMNS)
        if not ok:
            return 0, err
        return len(rows), ERR_NONE

    def upsert_document(self, filing_id: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
        existing, err = self._fetch_filing(filing_id)
        if err:
            return False, err
        if existing is None:
            return False, code(self.id, SUFFIX_WRITE_ERROR)
        merged = dict(existing)
        merged.update({column: payload.get(column) for column in _DOCUMENT_COLUMNS})
        merged["updated_at"] = datetime.now(timezone.utc)
        ok, err = self._insert(FILING_TABLE, [self._filing_values(merged)], _FILING_COLUMNS)
        if not ok:
            return False, err
        return True, ERR_NONE

    def mark_status(self, filing_id: str, status: str, reason: str = "") -> Tuple[bool, str]:
        existing, err = self._fetch_filing(filing_id)
        if err:
            return False, err
        if existing is None:
            return False, code(self.id, SUFFIX_WRITE_ERROR)
        merged = dict(existing)
        merged["document_status"] = status
        merged["document_status_reason"] = reason or ""
        merged["updated_at"] = datetime.now(timezone.utc)
        ok, err = self._insert(FILING_TABLE, [self._filing_values(merged)], _FILING_COLUMNS)
        if not ok:
            return False, err
        return True, ERR_NONE

    def insert_coverage(self, chunk: Dict[str, Any]) -> Tuple[bool, str]:
        row = [
            chunk.get("chunk_from"),
            chunk.get("chunk_to"),
            chunk.get("api_count", 0),
            chunk.get("ingested_count", 0),
            chunk.get("unique_count", 0),
            chunk.get("run_id", ""),
            chunk.get("timestamp") or datetime.now(timezone.utc),
        ]
        columns = [
            "chunk_from",
            "chunk_to",
            "api_count",
            "ingested_count",
            "unique_count",
            "run_id",
            "timestamp",
        ]
        ok, err = self._insert(COVERAGE_TABLE, [row], columns)
        return (True, ERR_NONE) if ok else (False, err)

    def upsert_edges(self, edges: List[Dict[str, Any]], kind: str) -> Tuple[int, str]:
        if kind not in EDGE_KINDS:
            return 0, code(self.id, SUFFIX_PAYLOAD_ERROR)
        if not edges:
            return 0, ERR_NONE
        now = datetime.now(timezone.utc)
        if kind == "has_filing":
            columns = ["company_id", "filing_id", "created_at"]
            rows = []
            for edge in edges:
                company_id = edge.get("company_id") or ticker_to_record_id(
                    edge.get("company_ticker", "")
                )
                filing_id = edge.get("filing_id", "")
                if company_id and filing_id:
                    rows.append([company_id, filing_id, now])
        else:
            columns = ["filing_id", "company_id", "source", "created_at"]
            rows = []
            for edge in edges:
                company_id = edge.get("company_id") or ticker_to_record_id(
                    edge.get("company_ticker", "")
                )
                filing_id = edge.get("filing_id", "")
                if company_id and filing_id:
                    rows.append(
                        [filing_id, company_id, edge.get("source", "title_extraction"), now]
                    )
        ok, err = self._insert(EDGE_TABLES[kind], rows, columns)
        if not ok:
            return 0, err
        # Insert-only: `created` is rows submitted; duplicates collapse on merge.
        return len(rows), ERR_NONE

    # -- reads -------------------------------------------------------------
    def read_filing_digests(self) -> Tuple[List[Dict[str, Any]], str]:
        rows, err = self._query(
            f"SELECT filing_id, document_sha256 FROM {FILING_TABLE} FINAL ORDER BY filing_id"
        )
        if err:
            return [], err
        return [
            {
                "filing_id": str(row.get("filing_id", "")),
                "document_sha256": row.get("document_sha256") or "",
            }
            for row in rows
        ], ERR_NONE

    def count_filings(self) -> Tuple[int, str]:
        return self._scalar(f"SELECT count() AS count FROM {FILING_TABLE} FINAL")

    def count_edges(self, kind: str) -> Tuple[int, str]:
        if kind not in EDGE_KINDS:
            return 0, code(self.id, SUFFIX_PAYLOAD_ERROR)
        return self._scalar(f"SELECT count() AS count FROM {EDGE_TABLES[kind]} FINAL")

    def count_pending_filings(self) -> Tuple[int, str]:
        return self._scalar(
            f"SELECT count() AS count FROM {FILING_TABLE} FINAL "
            "WHERE document_status IS NULL AND document_url IS NOT NULL "
            "AND document_url != ''"
        )

    def fetch_pending_filings(self, limit: int) -> Tuple[List[Dict[str, Any]], str]:
        return self._query(
            f"SELECT filing_id, document_url FROM {FILING_TABLE} FINAL "
            "WHERE document_status IS NULL AND document_url IS NOT NULL "
            "AND document_url != '' ORDER BY filing_date DESC LIMIT {limit:Int32}",
            {"limit": limit},
        )

    def distinct_company_tickers(self) -> Tuple[List[str], str]:
        rows, err = self._query(
            f"SELECT DISTINCT company_ticker FROM {FILING_TABLE} FINAL WHERE company_ticker != ''"
        )
        if err:
            return [], err
        return [r["company_ticker"] for r in rows if r.get("company_ticker")], ERR_NONE

    def fetch_filing_ids_by_ticker(self, tickers: List[str]) -> Tuple[List[Dict[str, Any]], str]:
        if not tickers:
            return [], ERR_NONE
        return self._query(
            f"SELECT company_ticker, filing_id FROM {FILING_TABLE} FINAL "
            "WHERE company_ticker IN {tickers:Array(String)}",
            {"tickers": list(tickers)},
        )

    def fetch_titles(
        self, ticker_set: Optional[List[str]], offset: int, page_size: int
    ) -> Tuple[List[Dict[str, Any]], str]:
        if ticker_set is not None:
            sql = (
                f"SELECT filing_id, title, stock_code, company_ticker FROM {FILING_TABLE} FINAL "
                "WHERE company_ticker IN {tickers:Array(String)} AND title IS NOT NULL "
                "ORDER BY filing_id ASC LIMIT {limit:Int32} OFFSET {offset:Int32}"
            )
            params: Dict[str, Any] = {
                "tickers": list(ticker_set),
                "limit": page_size,
                "offset": offset,
            }
        else:
            sql = (
                f"SELECT filing_id, title, stock_code, company_ticker FROM {FILING_TABLE} FINAL "
                "WHERE title IS NOT NULL "
                "ORDER BY filing_id ASC LIMIT {limit:Int32} OFFSET {offset:Int32}"
            )
            params = {"limit": page_size, "offset": offset}
        return self._query(sql, params)

    def fetch_coverage(self) -> Tuple[List[Dict[str, Any]], str]:
        return self._query(
            f"SELECT chunk_from, chunk_to, api_count, ingested_count, unique_count, run_id, "
            f"timestamp FROM {COVERAGE_TABLE} FINAL ORDER BY chunk_from DESC"
        )


__all__ = ["ClickHouseSink"]
