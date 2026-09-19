"""Neo4j sink — optional ``neo4j`` driver, graph model.

Nodes ``(:Filing {filingId})`` and ``(:Company {id})``; relationships
``(:Company)-[:HAS_FILING]->(:Filing)`` and
``(:Filing)-[:REFERENCES_FILING]->(:Company)``. All writes use ``MERGE`` so
re-running is idempotent. ``documentTables`` is stored as a JSON string because
Neo4j properties cannot hold nested maps.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from .. import config
from ..utils import ticker_to_record_id
from .base import (
    AGGREGATE_GROUPS,
    EDGE_KINDS,
    ERR_NONE,
    SUFFIX_DRIVER_MISSING,
    SUFFIX_DSN_MISSING,
    SUFFIX_PAYLOAD_ERROR,
    SUFFIX_SCHEMA_ERROR,
    SUFFIX_WRITE_ERROR,
    FilingQuery,
    Sink,
    SinkCapabilities,
    code,
    redact,
)

try:  # pragma: no cover - exercised via monkeypatching in tests
    import neo4j  # type: ignore

    _NEO4J_AVAILABLE = True
except Exception:  # pragma: no cover - depends on environment
    neo4j = None  # type: ignore
    _NEO4J_AVAILABLE = False

_CONSTRAINTS = (
    "CREATE CONSTRAINT filing_filing_id IF NOT EXISTS FOR (f:Filing) REQUIRE f.filingId IS UNIQUE",
    "CREATE CONSTRAINT company_id IF NOT EXISTS FOR (c:Company) REQUIRE c.id IS UNIQUE",
    "CREATE INDEX filing_ticker IF NOT EXISTS FOR (f:Filing) ON (f.companyTicker)",
    "CREATE INDEX filing_docstatus IF NOT EXISTS FOR (f:Filing) ON (f.documentStatus)",
    "CREATE INDEX filing_date IF NOT EXISTS FOR (f:Filing) ON (f.filingDate)",
    "CREATE INDEX coverage_run IF NOT EXISTS FOR (c:ScrapeCoverage) ON (c.runId)",
)

_UPSERT_FILINGS = (
    "UNWIND $records AS r "
    "MERGE (f:Filing {filingId: r.filing_id}) "
    "SET f.companyTicker = r.company_ticker, "
    "    f.stockCode = r.stock_code, "
    "    f.stockName = r.stock_name, "
    "    f.exchange = r.exchange, "
    "    f.filingType = r.filing_type, "
    "    f.filingSubtype = r.filing_subtype, "
    "    f.filingCategory = r.filing_category, "
    "    f.title = r.title, "
    "    f.filingDate = r.filing_date, "
    "    f.documentUrl = r.document_url, "
    "    f.referencedTickers = r.referenced_tickers, "
    "    f.source = r.source, "
    "    f.updatedAt = r.updated_at"
)

_UPSERT_EDGE = {
    "has_filing": (
        "UNWIND $edges AS e "
        "MERGE (c:Company {id: e.company_id}) "
        "MERGE (f:Filing {filingId: e.filing_id}) "
        "MERGE (c)-[r:HAS_FILING]->(f) "
        "ON CREATE SET r.createdAt = datetime()"
    ),
    "references_filing": (
        "UNWIND $edges AS e "
        "MERGE (f:Filing {filingId: e.filing_id}) "
        "MERGE (c:Company {id: e.company_id}) "
        "MERGE (f)-[r:REFERENCES_FILING]->(c) "
        "ON CREATE SET r.createdAt = datetime(), r.source = e.source"
    ),
}

_EDGE_REL = {"has_filing": "HAS_FILING", "references_filing": "REFERENCES_FILING"}


class Neo4jSink(Sink):
    id = "neo4j"
    capabilities = SinkCapabilities(
        model="graph",
        native_upsert=True,
        reads=True,
        edges=True,
        json=True,
        arrays=True,
        transactions=False,
        bulk=True,
    )

    def __init__(self) -> None:
        self._driver = None

    # -- lifecycle ---------------------------------------------------------
    def driver_installed(self) -> bool:
        return _NEO4J_AVAILABLE

    def configured(self) -> bool:
        return bool(config.NEO4J_URI and config.NEO4J_USER and config.NEO4J_PASSWORD)

    def available(self) -> bool:
        return self.driver_installed() and self.configured()

    def unavailable_reason(self) -> str:
        if not _NEO4J_AVAILABLE:
            return 'Neo4j sink requires the neo4j driver (install with: pip install ".[neo4j]")'
        if not config.NEO4J_URI:
            return "Neo4j sink requires NEO4J_URI"
        if not (config.NEO4J_USER and config.NEO4J_PASSWORD):
            return "Neo4j sink requires NEO4J_USER and NEO4J_PASSWORD"
        return "Neo4j sink is unavailable"

    def _get_driver(self) -> Tuple[Any, str]:
        if not _NEO4J_AVAILABLE or neo4j is None:
            return None, code(self.id, SUFFIX_DRIVER_MISSING)
        if not self.configured():
            return None, code(self.id, SUFFIX_DSN_MISSING)
        if self._driver is None:
            try:
                self._driver = neo4j.GraphDatabase.driver(
                    config.NEO4J_URI,
                    auth=(config.NEO4J_USER, config.NEO4J_PASSWORD),
                )
            except Exception as exc:  # noqa: BLE001
                return None, redact(str(exc)) or code(self.id, SUFFIX_WRITE_ERROR)
        return self._driver, ERR_NONE

    def _session(self):  # noqa: ANN201 - neo4j session
        driver, err = self._get_driver()
        if err:
            return None, err
        try:
            if config.NEO4J_DATABASE:
                return driver.session(database=config.NEO4J_DATABASE), ERR_NONE
            return driver.session(), ERR_NONE
        except Exception as exc:  # noqa: BLE001
            return None, redact(str(exc)) or code(self.id, SUFFIX_WRITE_ERROR)

    def _run(self, query: str, params: Optional[Dict[str, Any]] = None) -> Tuple[list, Any, str]:
        """Run a Cypher statement, returning ``(records, summary, error_code)``."""
        session, err = self._session()
        if err:
            return [], None, err
        try:
            result = session.run(query, **(params or {}))
            records = result.data()
            summary = result.consume()
            return records, summary, ERR_NONE
        except Exception as exc:  # noqa: BLE001
            return [], None, redact(str(exc)) or code(self.id, SUFFIX_WRITE_ERROR)
        finally:
            try:
                session.close()
            except Exception:  # pragma: no cover - best effort
                pass

    def ensure_schema(self) -> Tuple[bool, str]:
        for statement in _CONSTRAINTS:
            _records, _summary, err = self._run(statement)
            if err:
                return False, err or code(self.id, SUFFIX_SCHEMA_ERROR)
        return True, ERR_NONE

    def close(self) -> None:
        if self._driver is not None:
            try:
                self._driver.close()
            except Exception:  # pragma: no cover - best effort
                pass
            self._driver = None

    # -- writes ------------------------------------------------------------
    def upsert_filings(self, records: List[Dict[str, Any]]) -> Tuple[int, str]:
        if not records:
            return 0, ERR_NONE
        _records, _summary, err = self._run(_UPSERT_FILINGS, {"records": records})
        if err:
            return 0, err
        return len(records), ERR_NONE

    def upsert_document(self, filing_id: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
        query = (
            "MATCH (f:Filing {filingId: $fid}) "
            "SET f.documentSize = $size, f.documentType = $dtype, f.documentHash = $hash, "
            "    f.documentSha256 = $sha256, "
            "    f.documentText = $text, f.documentTextLen = $text_len, "
            "    f.documentTables = $tables, f.documentTableCnt = $table_cnt, "
            "    f.documentStatus = $status, f.documentStatusReason = $reason, "
            "    f.updatedAt = datetime() "
            "RETURN count(f) AS n"
        )
        params = {
            "fid": filing_id,
            "size": payload.get("document_size"),
            "dtype": payload.get("document_type"),
            "hash": payload.get("document_hash"),
            "sha256": payload.get("document_sha256"),
            "text": payload.get("document_text"),
            "text_len": payload.get("document_text_len"),
            "tables": json.dumps(payload.get("document_tables") or [], ensure_ascii=False),
            "table_cnt": payload.get("document_table_cnt"),
            "status": payload.get("document_status"),
            "reason": payload.get("document_status_reason"),
        }
        records, _summary, err = self._run(query, params)
        if err:
            return False, err
        if not records or not records[0].get("n"):
            return False, code(self.id, SUFFIX_WRITE_ERROR)
        return True, ERR_NONE

    def mark_status(self, filing_id: str, status: str, reason: str = "") -> Tuple[bool, str]:
        query = (
            "MATCH (f:Filing {filingId: $fid}) "
            "SET f.documentStatus = $status, f.documentStatusReason = $reason, "
            "    f.updatedAt = datetime() "
            "RETURN count(f) AS n"
        )
        records, _summary, err = self._run(
            query, {"fid": filing_id, "status": status, "reason": reason or ""}
        )
        if err:
            return False, err
        if not records or not records[0].get("n"):
            return False, code(self.id, SUFFIX_WRITE_ERROR)
        return True, ERR_NONE

    def insert_coverage(self, chunk: Dict[str, Any]) -> Tuple[bool, str]:
        key = f"{chunk.get('chunk_from')}|{chunk.get('chunk_to')}|{chunk.get('run_id', '')}"
        query = (
            "MERGE (c:ScrapeCoverage {key: $key}) "
            "SET c.chunkFrom = $chunk_from, c.chunkTo = $chunk_to, "
            "    c.apiCount = $api_count, c.ingestedCount = $ingested_count, "
            "    c.uniqueCount = $unique_count, c.runId = $run_id, "
            "    c.timestamp = $timestamp"
        )
        params = {
            "key": key,
            "chunk_from": str(chunk.get("chunk_from")),
            "chunk_to": str(chunk.get("chunk_to")),
            "api_count": chunk.get("api_count", 0),
            "ingested_count": chunk.get("ingested_count", 0),
            "unique_count": chunk.get("unique_count", 0),
            "run_id": chunk.get("run_id", ""),
            "timestamp": str(chunk.get("timestamp")),
        }
        _records, _summary, err = self._run(query, params)
        if err:
            return False, err
        return True, ERR_NONE

    def upsert_edges(self, edges: List[Dict[str, Any]], kind: str) -> Tuple[int, str]:
        if kind not in EDGE_KINDS:
            return 0, code(self.id, SUFFIX_PAYLOAD_ERROR)
        if not edges:
            return 0, ERR_NONE
        params_edges = []
        for edge in edges:
            company_id = edge.get("company_id") or ticker_to_record_id(
                edge.get("company_ticker", "")
            )
            filing_id = edge.get("filing_id", "")
            if not company_id or not filing_id:
                continue
            params_edges.append(
                {
                    "company_id": company_id,
                    "filing_id": filing_id,
                    "source": edge.get("source", "title_extraction"),
                }
            )
        if not params_edges:
            return 0, ERR_NONE
        _records, summary, err = self._run(_UPSERT_EDGE[kind], {"edges": params_edges})
        if err:
            return 0, err
        created = summary.counters.relationships_created if summary is not None else 0
        return created, ERR_NONE

    # -- reads -------------------------------------------------------------
    @staticmethod
    def _scalar(records: list, key: str = "count") -> int:
        if records and isinstance(records[0], dict):
            value = records[0].get(key, 0) or 0
            try:
                return int(value)
            except (TypeError, ValueError):
                return 0
        return 0

    def read_filing_digests(self) -> Tuple[List[Dict[str, Any]], str]:
        records, _summary, err = self._run(
            "MATCH (f:Filing) RETURN f.filingId AS filing_id, f.documentSha256 AS sha "
            "ORDER BY filing_id"
        )
        if err:
            return [], err
        return [
            {
                "filing_id": str(record.get("filing_id") or ""),
                "document_sha256": record.get("sha") or "",
            }
            for record in records
        ], ERR_NONE

    def count_filings(self) -> Tuple[int, str]:
        records, _summary, err = self._run("MATCH (f:Filing) RETURN count(f) AS count")
        if err:
            return 0, err
        return self._scalar(records), ERR_NONE

    def count_edges(self, kind: str) -> Tuple[int, str]:
        if kind not in EDGE_KINDS:
            return 0, code(self.id, SUFFIX_PAYLOAD_ERROR)
        rel = _EDGE_REL[kind]
        records, _summary, err = self._run(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS count")
        if err:
            return 0, err
        return self._scalar(records), ERR_NONE

    def count_pending_filings(self) -> Tuple[int, str]:
        query = (
            "MATCH (f:Filing) "
            "WHERE f.documentStatus IS NULL AND f.documentUrl IS NOT NULL AND f.documentUrl <> '' "
            "RETURN count(f) AS count"
        )
        records, _summary, err = self._run(query)
        if err:
            return 0, err
        return self._scalar(records), ERR_NONE

    def fetch_pending_filings(self, limit: int) -> Tuple[List[Dict[str, Any]], str]:
        query = (
            "MATCH (f:Filing) "
            "WHERE f.documentStatus IS NULL AND f.documentUrl IS NOT NULL AND f.documentUrl <> '' "
            "RETURN f.filingId AS filing_id, f.documentUrl AS document_url "
            "ORDER BY f.filingDate DESC LIMIT $limit"
        )
        records, _summary, err = self._run(query, {"limit": limit})
        if err:
            return [], err
        return records, ERR_NONE

    def distinct_company_tickers(self) -> Tuple[List[str], str]:
        query = (
            "MATCH (f:Filing) WHERE f.companyTicker IS NOT NULL "
            "RETURN DISTINCT f.companyTicker AS company_ticker"
        )
        records, _summary, err = self._run(query)
        if err:
            return [], err
        return [r["company_ticker"] for r in records if r.get("company_ticker")], ERR_NONE

    def fetch_filing_ids_by_ticker(self, tickers: List[str]) -> Tuple[List[Dict[str, Any]], str]:
        if not tickers:
            return [], ERR_NONE
        query = (
            "MATCH (f:Filing) WHERE f.companyTicker IN $tickers "
            "RETURN f.companyTicker AS company_ticker, f.filingId AS filing_id"
        )
        records, _summary, err = self._run(query, {"tickers": list(tickers)})
        if err:
            return [], err
        return records, ERR_NONE

    def fetch_titles(
        self,
        ticker_set: Optional[List[str]],
        offset: int,
        page_size: int,
        title_query: str = "",
    ) -> Tuple[List[Dict[str, Any]], str]:
        clauses = ["f.title IS NOT NULL"]
        params: Dict[str, Any] = {"offset": offset, "limit": page_size}
        if ticker_set is not None:
            clauses.append("f.companyTicker IN $tickers")
            params["tickers"] = list(ticker_set)
        if title_query:
            clauses.append("toLower(f.title) CONTAINS toLower($query)")
            params["query"] = title_query
        where = " AND ".join(clauses)
        query = (
            f"MATCH (f:Filing) WHERE {where} "
            "RETURN f.filingId AS filing_id, f.title AS title, "
            "       f.stockCode AS stock_code, f.companyTicker AS company_ticker "
            "ORDER BY f.filingId SKIP $offset LIMIT $limit"
        )
        records, _summary, err = self._run(query, params)
        if err:
            return [], err
        return records, ERR_NONE

    def fetch_filing_detail(self, filing_id: str) -> Tuple[Optional[Dict[str, Any]], str]:
        if not filing_id:
            return None, ERR_NONE
        query = (
            "MATCH (f:Filing {filingId: $id}) "
            "RETURN f.filingId AS filing_id, f.companyTicker AS company_ticker, "
            "       f.stockCode AS stock_code, f.stockName AS stock_name, "
            "       f.exchange AS exchange, f.filingType AS filing_type, "
            "       f.filingSubtype AS filing_subtype, f.filingCategory AS filing_category, "
            "       f.title AS title, f.filingDate AS filing_date, "
            "       f.documentUrl AS document_url, f.referencedTickers AS referenced_tickers, "
            "       f.source AS source, f.updatedAt AS updated_at, "
            "       f.documentSize AS document_size, f.documentType AS document_type, "
            "       f.documentHash AS document_hash, f.documentSha256 AS document_sha256, "
            "       f.documentText AS document_text, f.documentTextLen AS document_text_len, "
            "       f.documentTables AS document_tables, "
            "       f.documentTableCnt AS document_table_cnt, "
            "       f.documentStatus AS document_status, "
            "       f.documentStatusReason AS document_status_reason"
        )
        records, _summary, err = self._run(query, {"id": filing_id})
        if err:
            return None, err
        if not records:
            return None, ERR_NONE
        row = dict(records[0])
        tables = row.get("document_tables")
        if isinstance(tables, str) and tables:
            try:
                row["document_tables"] = json.loads(tables)
            except (ValueError, TypeError):
                pass
        return row, ERR_NONE

    @staticmethod
    def _search_where(query: FilingQuery) -> Tuple[List[str], Dict[str, Any]]:
        clauses: List[str] = []
        params: Dict[str, Any] = {}
        if query.tickers:
            clauses.append("f.companyTicker IN $tickers")
            params["tickers"] = list(query.tickers)
        if query.stock_codes:
            clauses.append("f.stockCode IN $stock_codes")
            params["stock_codes"] = list(query.stock_codes)
        if query.title_query:
            clauses.append("toLower(f.title) CONTAINS toLower($title)")
            params["title"] = query.title_query
        if query.text_query:
            clauses.append("toLower(f.documentText) CONTAINS toLower($text)")
            params["text"] = query.text_query
        if query.filing_types:
            clauses.append("f.filingType IN $ftypes")
            params["ftypes"] = list(query.filing_types)
        if query.filing_categories:
            clauses.append("f.filingCategory IN $fcats")
            params["fcats"] = list(query.filing_categories)
        if query.document_status:
            real = [s for s in query.document_status if s and s != "unprocessed"]
            parts: List[str] = []
            if real:
                parts.append("f.documentStatus IN $statuses")
                params["statuses"] = real
            if any(s == "unprocessed" for s in query.document_status):
                parts.append("f.documentStatus IS NULL")
            if parts:
                clauses.append("(" + " OR ".join(parts) + ")")
        if query.exchange:
            clauses.append("f.exchange = $exchange")
            params["exchange"] = query.exchange
        if query.source:
            clauses.append("f.source = $source")
            params["source"] = query.source
        if query.document_type:
            clauses.append("f.documentType = $dtype")
            params["dtype"] = query.document_type
        if query.referenced_ticker:
            clauses.append("ANY(t IN f.referencedTickers WHERE toLower(t) = toLower($refticker))")
            params["refticker"] = query.referenced_ticker
        if query.date_from:
            clauses.append("f.filingDate >= datetime($date_from)")
            params["date_from"] = query.date_from
        if query.date_to:
            clauses.append("f.filingDate < datetime($date_to) + duration({days: 1})")
            params["date_to"] = query.date_to
        return clauses, params

    _SEARCH_RETURN = (
        "f.filingId AS filing_id, f.companyTicker AS company_ticker, "
        "f.stockCode AS stock_code, f.stockName AS stock_name, f.exchange AS exchange, "
        "f.filingType AS filing_type, f.filingSubtype AS filing_subtype, "
        "f.filingCategory AS filing_category, f.title AS title, "
        "toString(f.filingDate) AS filing_date, f.documentUrl AS document_url, "
        "f.referencedTickers AS referenced_tickers, f.source AS source, "
        "toString(f.updatedAt) AS updated_at, f.documentStatus AS document_status, "
        "f.documentType AS document_type, f.documentTextLen AS document_text_len, "
        "f.documentTableCnt AS document_table_cnt"
    )

    _ORDER = {
        "filing_date_desc": "f.filingDate DESC, f.filingId ASC",
        "filing_date_asc": "f.filingDate ASC, f.filingId ASC",
        "title_asc": "f.title ASC, f.filingId ASC",
        "filing_id_asc": "f.filingId ASC",
    }

    def search_filings(
        self, query: FilingQuery, offset: int, limit: int
    ) -> Tuple[List[Dict[str, Any]], str]:
        clauses, params = self._search_where(query)
        where = " AND ".join(clauses) if clauses else "true"
        params.update({"offset": offset, "limit": limit})
        order = self._ORDER.get(query.order_by, self._ORDER["filing_date_desc"])
        query_text = (
            f"MATCH (f:Filing) WHERE {where} RETURN {self._SEARCH_RETURN} "
            f"ORDER BY {order} SKIP $offset LIMIT $limit"
        )
        records, _summary, err = self._run(query_text, params)
        if err:
            return [], err
        return records, ERR_NONE

    def search_documents(
        self, query: FilingQuery, offset: int, limit: int
    ) -> Tuple[List[Dict[str, Any]], str]:
        rows, err = self.search_filings(query, offset, limit)
        if err:
            return [], err
        for row in rows:
            row["snippet"] = None
        return rows, ERR_NONE

    def aggregate_filings(
        self, group_by: str, query: FilingQuery
    ) -> Tuple[List[Dict[str, Any]], str]:
        if group_by not in AGGREGATE_GROUPS:
            return [], code(self.id, SUFFIX_PAYLOAD_ERROR)
        prop = {
            "company_ticker": "companyTicker",
            "filing_type": "filingType",
            "filing_category": "filingCategory",
            "document_status": "documentStatus",
            "exchange": "exchange",
        }[group_by]
        clauses, params = self._search_where(query)
        where = " AND ".join(clauses) if clauses else "true"
        query_text = (
            f"MATCH (f:Filing) WHERE {where} RETURN f.{prop} AS key, count(*) AS count "
            "ORDER BY count DESC, key ASC LIMIT 200"
        )
        records, _summary, err = self._run(query_text, params)
        if err:
            return [], err
        return [{"key": r.get("key"), "count": int(r.get("count") or 0)} for r in records], ERR_NONE

    def list_companies(self, limit: int, offset: int) -> Tuple[List[Dict[str, Any]], str]:
        query_text = (
            "MATCH (f:Filing) WHERE f.companyTicker IS NOT NULL AND f.companyTicker <> '' "
            "RETURN f.companyTicker AS company_ticker, head(collect(f.stockName)) AS stock_name, "
            "count(*) AS filing_count ORDER BY filing_count DESC, company_ticker ASC "
            "SKIP $offset LIMIT $limit"
        )
        records, _summary, err = self._run(query_text, {"offset": offset, "limit": limit})
        if err:
            return [], err
        return records, ERR_NONE

    def fetch_coverage(self) -> Tuple[List[Dict[str, Any]], str]:
        query = (
            "MATCH (c:ScrapeCoverage) "
            "RETURN c.chunkFrom AS chunk_from, c.chunkTo AS chunk_to, "
            "       c.apiCount AS api_count, c.ingestedCount AS ingested_count, "
            "       c.uniqueCount AS unique_count, c.runId AS run_id, c.timestamp AS timestamp "
            "ORDER BY c.chunkFrom DESC"
        )
        records, _summary, err = self._run(query)
        if err:
            return [], err
        return records, ERR_NONE
