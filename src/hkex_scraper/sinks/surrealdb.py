"""SurrealDB sink — adapter over :mod:`hkex_scraper.db`.

SurrealDB is a graph+document store, so it does not fit the row-insert shape of
the relational sinks. This adapter owns its SurrealQL for filings, statuses,
coverage, and edges, its record-link and payload-size fallbacks for document
saves, and its read routing. The pipeline and graph linker treat it like any
other sink.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from .. import db
from ..config import (
    COMPANY_TABLE,
    MAX_RPC_BODY_SIZE,
    MAX_SQL_BODY_SIZE,
    SURREAL_ENDPOINT,
    SURREAL_PASS,
)
from ..utils import escape_sql, log, normalize_company_id, squash_ws, ticker_to_record_id
from .base import (
    AGGREGATE_GROUPS,
    EDGE_KINDS,
    ERR_NONE,
    SUFFIX_DISABLED,
    SUFFIX_PAYLOAD_ERROR,
    SUFFIX_WRITE_ERROR,
    FilingQuery,
    Sink,
    SinkCapabilities,
    code,
)
from .dialects import COVERAGE_COLUMNS


def _first_result_rows(result: Any) -> list:
    """Extract the row list from a ``/sql`` response, tolerating every shape."""
    if isinstance(result, list) and result:
        entry = result[0]
        if isinstance(entry, dict):
            rows = entry.get("result", [])
            if isinstance(rows, list):
                return rows
    return []


# SurrealDB stores documents with camelCase keys; the read contract is snake_case.
_DETAIL_FIELD_MAP = {
    "filingId": "filing_id",
    "companyTicker": "company_ticker",
    "stockCode": "stock_code",
    "stockName": "stock_name",
    "exchange": "exchange",
    "filingType": "filing_type",
    "filingSubtype": "filing_subtype",
    "filingCategory": "filing_category",
    "title": "title",
    "filingDate": "filing_date",
    "documentUrl": "document_url",
    "referencedTickers": "referenced_tickers",
    "source": "source",
    "updatedAt": "updated_at",
    "documentSize": "document_size",
    "documentType": "document_type",
    "documentHash": "document_hash",
    "documentSha256": "document_sha256",
    "documentText": "document_text",
    "documentTextLen": "document_text_len",
    "documentTables": "document_tables",
    "documentTableCnt": "document_table_cnt",
    "documentStatus": "document_status",
    "documentStatusReason": "document_status_reason",
}


def _detail_from_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Map a SurrealDB filing record onto the canonical snake_case detail shape."""
    return {target: row[source] for source, target in _DETAIL_FIELD_MAP.items() if source in row}


# The subset of fields returned by the composable search surface.
_SEARCH_FIELD_MAP = {
    key: value
    for key, value in _DETAIL_FIELD_MAP.items()
    if value
    in {
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
    }
}
_SEARCH_SELECT = ", ".join(_SEARCH_FIELD_MAP)
_GROUP_PROP = {
    "company_ticker": "companyTicker",
    "filing_type": "filingType",
    "filing_category": "filingCategory",
    "document_status": "documentStatus",
    "exchange": "exchange",
}
_ORDER = {
    "filing_date_desc": "filingDate DESC, filingId ASC",
    "filing_date_asc": "filingDate ASC, filingId ASC",
    "title_asc": "title ASC, filingId ASC",
    "filing_id_asc": "filingId ASC",
}


def _search_where(query: FilingQuery) -> str:
    clauses: List[str] = []
    if query.tickers:
        items = ", ".join(f"'{escape_sql(t)}'" for t in query.tickers)
        clauses.append(f"companyTicker IN [{items}]")
    if query.stock_codes:
        items = ", ".join(f"'{escape_sql(c)}'" for c in query.stock_codes)
        clauses.append(f"stockCode IN [{items}]")
    if query.title_query:
        clauses.append(
            f"string::lowercase(title) CONTAINS '{escape_sql(query.title_query.lower())}'"
        )
    if query.text_query:
        needle = escape_sql(query.text_query.lower())
        clauses.append(f"string::lowercase(documentText) CONTAINS '{needle}'")
    if query.filing_types:
        items = ", ".join(f"'{escape_sql(t)}'" for t in query.filing_types)
        clauses.append(f"filingType IN [{items}]")
    if query.filing_categories:
        items = ", ".join(f"'{escape_sql(c)}'" for c in query.filing_categories)
        clauses.append(f"filingCategory IN [{items}]")
    if query.document_status:
        real = [s for s in query.document_status if s and s != "unprocessed"]
        parts: List[str] = []
        if real:
            items = ", ".join(f"'{escape_sql(s)}'" for s in real)
            parts.append(f"documentStatus IN [{items}]")
        if any(s == "unprocessed" for s in query.document_status):
            parts.append("documentStatus IS NONE")
        if parts:
            clauses.append("(" + " OR ".join(parts) + ")")
    if query.exchange:
        clauses.append(f"exchange = '{escape_sql(query.exchange)}'")
    if query.source:
        clauses.append(f"source = '{escape_sql(query.source)}'")
    if query.document_type:
        clauses.append(f"documentType = '{escape_sql(query.document_type)}'")
    if query.referenced_ticker:
        clauses.append(f"referencedTickers CONTAINS '{escape_sql(query.referenced_ticker)}'")
    if query.date_from:
        clauses.append(f"filingDate >= d'{escape_sql(query.date_from)}'")
    if query.date_to:
        clauses.append(f"filingDate <= d'{escape_sql(query.date_to)}'")
    return " AND ".join(clauses) if clauses else "true"


class SurrealDBSink(Sink):
    id = "surrealdb"
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
        self._company_ids: Optional[Dict[str, str]] = None

    # -- lifecycle ---------------------------------------------------------
    def available(self) -> bool:
        return bool(SURREAL_ENDPOINT and SURREAL_PASS)

    def unavailable_reason(self) -> str:
        missing = []
        if not SURREAL_ENDPOINT:
            missing.append("SURREAL_ENDPOINT")
        if not SURREAL_PASS:
            missing.append("SURREAL_PASSWORD")
        return f"SurrealDB sink requires {', '.join(missing)}"

    def ensure_schema(self) -> Tuple[bool, str]:
        ok = db.initialize_schema()
        return (True, ERR_NONE) if ok else (False, code(self.id, "SCHEMA_ERROR"))

    # -- company record resolution ----------------------------------------
    def _load_company_ids(self) -> Dict[str, str]:
        """Map normalised company key -> SurrealDB record id (cached)."""
        if self._company_ids is not None:
            return self._company_ids
        company_ids: Dict[str, str] = {}
        if not COMPANY_TABLE:
            self._company_ids = company_ids
            return company_ids
        result = db.surreal_query(f"SELECT id FROM {COMPANY_TABLE};", timeout=60)
        if isinstance(result, dict) and result.get("error"):
            log(f"  Could not load company IDs: {str(result['error'])[:200]}")
            self._company_ids = company_ids
            return company_ids
        for row in _first_result_rows(result):
            if not isinstance(row, dict):
                continue
            cid = str(row.get("id", ""))
            if cid:
                company_ids[normalize_company_id(cid)] = cid
        log(f"  Loaded {len(company_ids)} company IDs for matching")
        self._company_ids = company_ids
        return company_ids

    # -- writes ------------------------------------------------------------
    def upsert_filings(self, records: List[Dict[str, Any]]) -> Tuple[int, str]:
        if not records:
            return 0, ERR_NONE
        statements = [self._filing_upsert(f) for f in records]
        saved = db.upsert_batch_with_retry(statements)
        if saved < len(statements):
            return saved, code(self.id, SUFFIX_WRITE_ERROR)
        return saved, ERR_NONE

    @staticmethod
    def _filing_upsert(f: Dict[str, Any]) -> str:
        filing_date = f.get("filing_date")
        date_expr = f"d'{filing_date.strftime('%Y-%m-%d')}'" if filing_date else "NULL"
        ref_tickers = json.dumps(f.get("referenced_tickers") or [])
        return (
            "UPSERT exchange_filing:{fid} SET\n"
            "  filingId       = '{fid}',\n"
            "  companyTicker  = '{ticker}',\n"
            "  stockCode      = '{stockCode}',\n"
            "  stockName      = '{stockName}',\n"
            "  exchange       = 'HK',\n"
            "  filingType     = '{ft}',\n"
            "  filingSubtype  = '{fs}',\n"
            "  filingCategory = '{filingCategory}',\n"
            "  title          = '{title}',\n"
            "  filingDate     = {filingDateExpr},\n"
            "  documentUrl    = '{docUrl}',\n"
            "  referencedTickers = {refTickers},\n"
            "  source         = 'HKEx',\n"
            "  updatedAt      = time::now()\n"
            "RETURN NONE;\n".format(
                fid=f["filing_id"],
                ticker=f.get("company_ticker", ""),
                stockCode=escape_sql(f.get("stock_code", "")),
                stockName=escape_sql(squash_ws(f.get("stock_name") or "")),
                ft=f.get("filing_type", "Other"),
                fs=escape_sql(f.get("filing_subtype") or ""),
                filingCategory=f.get("filing_category") or "",
                title=escape_sql(squash_ws(f.get("title") or "")),
                filingDateExpr=date_expr,
                docUrl=escape_sql(f.get("document_url") or ""),
                refTickers=ref_tickers,
            )
        )

    def upsert_document(self, filing_id: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
        """Save a document payload via ``/rpc`` with size and record-link fallbacks.

        The caller supplies the full canonical payload; SurrealDB applies its own
        RPC body-size limit here (see :class:`SinkCapabilities`).
        """
        doc_type = payload.get("document_type") or "unknown"
        size_bytes = payload.get("document_size") or 0
        doc_hash = payload.get("document_hash") or ""
        extracted_text = payload.get("document_text") or ""
        tables_list = [
            {k: v for k, v in tbl.items() if v is not None}
            for tbl in (payload.get("document_tables") or [])
        ]
        status = payload.get("document_status") or "processed"
        reason = payload.get("document_status_reason") or ""

        original_text_len = len(extracted_text)
        text_to_save = extracted_text
        was_truncated = False

        rpc_overhead = 2048
        tables_json_str = json.dumps(tables_list, ensure_ascii=False)
        tables_bytes = len(tables_json_str.encode("utf-8"))
        estimated_total = (
            int(len(text_to_save.encode("utf-8")) * 1.05) + tables_bytes + rpc_overhead
        )

        if estimated_total > MAX_RPC_BODY_SIZE:
            available_for_text = MAX_RPC_BODY_SIZE - tables_bytes - rpc_overhead
            if available_for_text > 100_000:
                target_chars = int(available_for_text / 1.05)
                text_to_save = extracted_text[:target_chars]
                last_nl = text_to_save.rfind("\n")
                if last_nl > target_chars // 2:
                    text_to_save = text_to_save[:last_nl]
                was_truncated = True
                log(
                    f"  Pre-truncated text for {filing_id}: {original_text_len} -> "
                    f"{len(text_to_save)} chars (tables: {tables_bytes} bytes kept)"
                )
            else:
                kept: list = []
                running = 2
                for tbl in tables_list:
                    blob = json.dumps(tbl, ensure_ascii=False)
                    if running + len(blob.encode("utf-8")) + 2 > 1_000_000:
                        break
                    kept.append(tbl)
                    running += len(blob.encode("utf-8")) + 2
                tables_list = kept
                tables_bytes = len(json.dumps(tables_list, ensure_ascii=False).encode("utf-8"))
                available_for_text = MAX_RPC_BODY_SIZE - tables_bytes - rpc_overhead
                target_chars = max(int(available_for_text / 1.05), 100_000)
                text_to_save = extracted_text[:target_chars]
                last_nl = text_to_save.rfind("\n")
                if last_nl > target_chars // 2:
                    text_to_save = text_to_save[:last_nl]
                was_truncated = True

        reason_final = f"truncated_from_{original_text_len}" if was_truncated else reason

        sql_template = (
            f"UPDATE exchange_filing:{filing_id} SET "
            "documentSize = $doc_size, "
            "documentType = $doc_type, "
            "documentHash = $doc_hash, "
            "documentText = $doc_text, "
            "documentTextLen = $doc_text_len, "
            "documentTables = $doc_tables, "
            "documentTableCnt = $doc_table_cnt, "
            "documentStatus = $doc_status, "
            "documentStatusReason = $doc_reason, "
            "updatedAt = time::now() "
            "RETURN NONE;"
        )
        vars_dict = {
            "doc_size": size_bytes,
            "doc_type": doc_type,
            "doc_hash": doc_hash,
            "doc_text": text_to_save,
            "doc_text_len": len(text_to_save),
            "doc_tables": tables_list,
            "doc_table_cnt": len(tables_list),
            "doc_status": status,
            "doc_reason": reason_final,
        }

        result = db.surreal_rpc("query", [sql_template, vars_dict], timeout=120)
        if not (isinstance(result, dict) and result.get("error")):
            return True, ERR_NONE

        err = result.get("error", "unknown") if isinstance(result, dict) else "unknown"

        if _is_record_link_error(result):
            log(f"  RPC record-link error for {filing_id} ({str(err)[:80]}): retrying via /sql")
            return self._save_via_sql(
                filing_id,
                text_to_save,
                tables_list,
                doc_type,
                size_bytes,
                doc_hash,
                status,
                reason_final,
            )

        if _is_body_too_large(result):
            limit = 2_000_000
            truncated = extracted_text[:limit]
            last_nl = truncated.rfind("\n")
            if last_nl > limit // 2:
                truncated = truncated[:last_nl]
            log(f"  RPC payload too large for {filing_id}: retrying with {len(truncated)} chars")
            vars_dict["doc_text"] = truncated
            vars_dict["doc_text_len"] = len(truncated)
            vars_dict["doc_tables"] = []
            vars_dict["doc_table_cnt"] = 0
            vars_dict["doc_reason"] = f"truncated_from_{original_text_len}"
            retry = db.surreal_rpc("query", [sql_template, vars_dict], timeout=120)
            if not (isinstance(retry, dict) and retry.get("error")):
                return True, ERR_NONE
            log(f"  Doc save failed for {filing_id} after RPC truncation")
            return False, code(self.id, SUFFIX_WRITE_ERROR)

        log(f"  Doc save failed for {filing_id}: {str(err)[:200]}")
        return False, f"{code(self.id, SUFFIX_WRITE_ERROR)}:{str(err)[:100]}"

    def _save_via_sql(
        self,
        filing_id: str,
        text: str,
        tables: list,
        doc_type: str,
        size_bytes: int,
        doc_hash: str,
        status: str,
        reason: str,
    ) -> Tuple[bool, str]:
        tables_json = json.dumps(tables, ensure_ascii=False)
        escaped_text = escape_sql(text)
        sql = (
            f"UPDATE exchange_filing:{filing_id} SET "
            f"documentSize = {size_bytes}, "
            f"documentType = '{escape_sql(doc_type)}', "
            f"documentHash = '{escape_sql(doc_hash)}', "
            f"documentText = '{escaped_text}', "
            f"documentTextLen = {len(text)}, "
            f"documentTables = {tables_json}, "
            f"documentTableCnt = {len(tables)}, "
            f"documentStatus = '{escape_sql(status)}', "
            f"documentStatusReason = '{escape_sql(reason)}', "
            f"updatedAt = time::now() "
            f"RETURN NONE;"
        )
        sql_bytes = len(sql.encode("utf-8"))
        if sql_bytes > MAX_SQL_BODY_SIZE:
            overhead = sql_bytes - len(escaped_text.encode("utf-8"))
            budget = MAX_SQL_BODY_SIZE - overhead - 4096
            trunc = text[: max(budget, 50_000)]
            last_nl = trunc.rfind("\n")
            if last_nl > len(trunc) // 2:
                trunc = trunc[:last_nl]
            sql = (
                f"UPDATE exchange_filing:{filing_id} SET "
                f"documentSize = {size_bytes}, "
                f"documentType = '{escape_sql(doc_type)}', "
                f"documentHash = '{escape_sql(doc_hash)}', "
                f"documentText = '{escape_sql(trunc)}', "
                f"documentTextLen = {len(trunc)}, "
                f"documentTables = [], "
                f"documentTableCnt = 0, "
                f"documentStatus = 'processed', "
                f"documentStatusReason = 'sql_fallback_truncated_from_{len(text)}', "
                f"updatedAt = time::now() "
                f"RETURN NONE;"
            )
            log(f"  /sql fallback truncated text for {filing_id}: {len(text)} -> {len(trunc)}")

        sql_result = db.surreal_query(sql, timeout=120)
        if isinstance(sql_result, list):
            for entry in sql_result:
                if isinstance(entry, dict) and entry.get("status") == "ERR":
                    log(f"  /sql fallback failed for {filing_id}: {str(entry.get('result'))[:200]}")
                    return False, code(self.id, SUFFIX_WRITE_ERROR)
            return True, ERR_NONE
        log(f"  /sql fallback failed for {filing_id}: {str(sql_result)[:200]}")
        return False, code(self.id, SUFFIX_WRITE_ERROR)

    def mark_status(self, filing_id: str, status: str, reason: str = "") -> Tuple[bool, str]:
        sql = (
            "UPDATE exchange_filing:{fid} SET\n"
            "  documentStatus = '{status}',\n"
            "  documentStatusReason = '{reason}',\n"
            "  updatedAt = time::now()\n"
            "RETURN NONE;\n"
        ).format(
            fid=filing_id,
            status=escape_sql(status),
            reason=escape_sql(reason[:200]) if reason else "",
        )
        result = db.surreal_query(sql, timeout=30)
        if isinstance(result, dict) and result.get("error"):
            return False, code(self.id, SUFFIX_WRITE_ERROR)
        return True, ERR_NONE

    def insert_coverage(self, chunk: Dict[str, Any]) -> Tuple[bool, str]:
        chunk_from = chunk.get("chunk_from")
        chunk_to = chunk.get("chunk_to")

        def _d(value: Any) -> str:
            if hasattr(value, "strftime"):
                return f"d'{value.strftime('%Y-%m-%d')}'"
            return f"d'{value}'"

        sql = (
            "INSERT INTO scrape_coverage {"
            f"  chunkFrom: {_d(chunk_from)}, "
            f"  chunkTo: {_d(chunk_to)}, "
            f"  apiCount: {chunk.get('api_count', 0)}, "
            f"  ingestedCount: {chunk.get('ingested_count', 0)}, "
            f"  uniqueCount: {chunk.get('unique_count', 0)}, "
            f"  runId: '{escape_sql(chunk.get('run_id', ''))}', "
            "  timestamp: time::now()"
            "};"
        )
        result = db.surreal_query(sql, timeout=30)
        if isinstance(result, dict) and result.get("error"):
            return False, code(self.id, SUFFIX_WRITE_ERROR)
        return True, ERR_NONE

    def upsert_edges(self, edges: List[Dict[str, Any]], kind: str) -> Tuple[int, str]:
        if kind not in EDGE_KINDS:
            return 0, code(self.id, SUFFIX_PAYLOAD_ERROR)
        if not edges:
            return 0, ERR_NONE
        company_ids = self._load_company_ids()

        statements: List[str] = []
        linked = 0
        skipped = 0
        for edge in edges:
            ticker = edge.get("company_ticker", "")
            filing_id = edge.get("filing_id", "")
            if not ticker or not filing_id:
                skipped += 1
                continue
            record_key = ticker_to_record_id(ticker)
            db_id = company_ids.get(record_key)
            if not db_id:
                skipped += 1
                continue
            if kind == "has_filing":
                statements.append(
                    f"RELATE ({db_id})->has_filing->(exchange_filing:{filing_id})"
                    f" SET createdAt = time::now() RETURN NONE;"
                )
            else:
                statements.append(
                    f"RELATE (exchange_filing:{filing_id})->references_filing->({db_id})"
                    f" SET createdAt = time::now(), source = 'title_extraction' RETURN NONE;"
                )
            linked += 1

        if skipped:
            log(f"  SurrealDB {kind}: {skipped} edge(s) skipped (no company match)")

        created = 0
        batch = 50
        for start in range(0, len(statements), batch):
            chunk = statements[start : start + batch]
            result = db.surreal_query("\n".join(chunk), timeout=300)
            if isinstance(result, dict) and result.get("error"):
                log(f"  SurrealDB {kind} batch error at {start}: {str(result['error'])[:200]}")
            else:
                created += len(chunk)
        return created, ERR_NONE

    # -- reads -------------------------------------------------------------
    def _count(self, sql: str) -> Tuple[int, str]:
        result = db.surreal_query(sql, timeout=60)
        rows = _first_result_rows(result)
        if rows and isinstance(rows[0], dict):
            value = rows[0].get("count") or rows[0].get("cnt") or 0
            try:
                return int(value), ERR_NONE
            except (TypeError, ValueError):
                return 0, code(self.id, SUFFIX_WRITE_ERROR)
        return 0, ERR_NONE

    def read_filing_digests(self) -> Tuple[List[Dict[str, Any]], str]:
        result = db.surreal_query(
            "SELECT filingId, documentSha256 FROM exchange_filing;", timeout=120
        )
        if isinstance(result, dict) and result.get("error"):
            return [], code(self.id, SUFFIX_WRITE_ERROR)
        rows = _first_result_rows(result)
        digests = [
            {
                "filing_id": str(row.get("filingId") or row.get("filingID") or ""),
                "document_sha256": row.get("documentSha256") or "",
            }
            for row in rows
        ]
        digests.sort(key=lambda row: row["filing_id"])
        return digests, ERR_NONE

    def count_filings(self) -> Tuple[int, str]:
        return self._count("SELECT count() FROM exchange_filing GROUP ALL;")

    def count_edges(self, kind: str) -> Tuple[int, str]:
        if kind not in EDGE_KINDS:
            return 0, code(self.id, SUFFIX_PAYLOAD_ERROR)
        return self._count(f"SELECT count() FROM {kind} GROUP ALL;")

    def count_pending_filings(self) -> Tuple[int, str]:
        return self._count(
            "SELECT count() AS count FROM exchange_filing "
            "WHERE documentStatus IS NONE AND documentUrl IS NOT NONE GROUP ALL;"
        )

    def fetch_pending_filings(self, limit: int) -> Tuple[List[Dict[str, Any]], str]:
        result = db.surreal_query(
            f"SELECT filingId, documentUrl FROM exchange_filing "
            f"WHERE documentStatus IS NONE AND documentUrl IS NOT NONE "
            f"ORDER BY filingDate DESC LIMIT {limit};",
            timeout=120,
        )
        if isinstance(result, dict) and result.get("error"):
            return [], code(self.id, SUFFIX_WRITE_ERROR)
        rows = []
        for row in _first_result_rows(result):
            if not isinstance(row, dict):
                continue
            rows.append(
                {
                    "filing_id": row.get("filingId", ""),
                    "document_url": row.get("documentUrl", ""),
                }
            )
        return rows, ERR_NONE

    def distinct_company_tickers(self) -> Tuple[List[str], str]:
        result = db.surreal_query(
            "SELECT companyTicker FROM exchange_filing "
            "WHERE companyTicker IS NOT NONE GROUP BY companyTicker;",
            timeout=120,
        )
        if isinstance(result, dict) and result.get("error"):
            return [], code(self.id, SUFFIX_WRITE_ERROR)
        return [
            row.get("companyTicker", "")
            for row in _first_result_rows(result)
            if isinstance(row, dict) and row.get("companyTicker")
        ], ERR_NONE

    def fetch_filing_ids_by_ticker(self, tickers: List[str]) -> Tuple[List[Dict[str, Any]], str]:
        if not tickers:
            return [], ERR_NONE
        ticker_list = ", ".join(f"'{escape_sql(t)}'" for t in tickers)
        result = db.surreal_query(
            f"SELECT companyTicker, filingId FROM exchange_filing "
            f"WHERE companyTicker IN [{ticker_list}];",
            timeout=300,
        )
        if isinstance(result, dict) and result.get("error"):
            return [], code(self.id, SUFFIX_WRITE_ERROR)
        return [
            {"company_ticker": row.get("companyTicker", ""), "filing_id": row.get("filingId", "")}
            for row in _first_result_rows(result)
            if isinstance(row, dict)
        ], ERR_NONE

    def fetch_titles(
        self,
        ticker_set: Optional[List[str]],
        offset: int,
        page_size: int,
        title_query: str = "",
    ) -> Tuple[List[Dict[str, Any]], str]:
        clauses = ["title IS NOT NONE"]
        if ticker_set is not None:
            ticker_list = ", ".join(f"'{escape_sql(t)}'" for t in ticker_set)
            clauses.append(f"companyTicker IN [{ticker_list}]")
        if title_query:
            clauses.append(f"string::lowercase(title) CONTAINS {escape_sql(title_query.lower())}")
        where = " AND ".join(clauses)
        sql = (
            f"SELECT filingId, title, stockCode, companyTicker FROM exchange_filing "
            f"WHERE {where} ORDER BY filingId ASC START {offset} LIMIT {page_size};"
        )
        result = db.surreal_query(sql, timeout=300)
        if isinstance(result, dict) and result.get("error"):
            return [], code(self.id, SUFFIX_WRITE_ERROR)
        return [
            {
                "filing_id": row.get("filingId", ""),
                "title": row.get("title", ""),
                "stock_code": row.get("stockCode", ""),
                "company_ticker": row.get("companyTicker", ""),
            }
            for row in _first_result_rows(result)
            if isinstance(row, dict)
        ], ERR_NONE

    def fetch_filing_detail(self, filing_id: str) -> Tuple[Optional[Dict[str, Any]], str]:
        if not filing_id:
            return None, ERR_NONE
        result = db.surreal_query(
            f"SELECT * FROM exchange_filing WHERE filingId = '{escape_sql(filing_id)}' LIMIT 1;",
            timeout=120,
        )
        if isinstance(result, dict) and result.get("error"):
            return None, code(self.id, SUFFIX_WRITE_ERROR)
        rows = [row for row in _first_result_rows(result) if isinstance(row, dict)]
        if not rows:
            return None, ERR_NONE
        return _detail_from_row(rows[0]), ERR_NONE

    def search_filings(
        self, query: FilingQuery, offset: int, limit: int
    ) -> Tuple[List[Dict[str, Any]], str]:
        where = _search_where(query)
        order = _ORDER.get(query.order_by, _ORDER["filing_date_desc"])
        sql = (
            f"SELECT {_SEARCH_SELECT} FROM exchange_filing WHERE {where} "
            f"ORDER BY {order} START {offset} LIMIT {limit};"
        )
        result = db.surreal_query(sql, timeout=300)
        if isinstance(result, dict) and result.get("error"):
            return [], code(self.id, SUFFIX_WRITE_ERROR)
        return [
            _detail_from_row(row)
            for row in _first_result_rows(result)
            if isinstance(row, dict) and row.get("filingId") is not None
        ], ERR_NONE

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
        prop = _GROUP_PROP[group_by]
        where = _search_where(query)
        sql = (
            f"SELECT {prop} AS key, count() AS cnt FROM exchange_filing WHERE {where} "
            f"GROUP BY {prop} ORDER BY cnt DESC LIMIT 200;"
        )
        result = db.surreal_query(sql, timeout=300)
        if isinstance(result, dict) and result.get("error"):
            return [], code(self.id, SUFFIX_WRITE_ERROR)
        return [
            {"key": row.get("key"), "count": int(row.get("cnt") or 0)}
            for row in _first_result_rows(result)
            if isinstance(row, dict)
        ], ERR_NONE

    def list_companies(
        self, limit: int, offset: int, ticker: str = ""
    ) -> Tuple[List[Dict[str, Any]], str]:
        # ``stock_name`` is not aggregated (SurrealDB has no string max); it is null here.
        where = "companyTicker IS NOT NONE"
        if ticker:
            where += (
                f" AND string::lowercase(companyTicker) "
                f"CONTAINS string::lowercase('{escape_sql(ticker)}')"
            )
        sql = (
            "SELECT companyTicker, count() AS cnt FROM exchange_filing "
            f"WHERE {where} "
            f"GROUP BY companyTicker ORDER BY cnt DESC START {offset} LIMIT {limit};"
        )
        result = db.surreal_query(sql, timeout=300)
        if isinstance(result, dict) and result.get("error"):
            return [], code(self.id, SUFFIX_WRITE_ERROR)
        return [
            {
                "company_ticker": row.get("companyTicker", ""),
                "stock_name": None,
                "filing_count": int(row.get("cnt") or 0),
            }
            for row in _first_result_rows(result)
            if isinstance(row, dict)
        ], ERR_NONE

    def fetch_coverage(self) -> Tuple[List[Dict[str, Any]], str]:
        result = db.surreal_query(
            "SELECT chunkFrom, chunkTo, apiCount, ingestedCount, uniqueCount, runId, timestamp "
            "FROM scrape_coverage ORDER BY chunkFrom DESC;",
            timeout=30,
        )
        if isinstance(result, dict) and result.get("error"):
            return [], code(self.id, SUFFIX_WRITE_ERROR)
        return [
            {
                "chunk_from": row.get("chunkFrom"),
                "chunk_to": row.get("chunkTo"),
                "api_count": row.get("apiCount", 0),
                "ingested_count": row.get("ingestedCount", 0),
                "unique_count": row.get("uniqueCount", 0),
                "run_id": row.get("runId", ""),
                "timestamp": row.get("timestamp"),
            }
            for row in _first_result_rows(result)
            if isinstance(row, dict)
        ], ERR_NONE


# ---------------------------------------------------------------------------
# Error-shape detectors (SurrealDB-specific)
# ---------------------------------------------------------------------------

_BODY_TOO_LARGE_SIGNALS = (
    "10053",
    "10054",
    "ECONNRESET",
    "Connection aborted",
    "Connection reset",
    "RemoteDisconnected",
    "BrokenPipeError",
)


def _is_record_link_error(result: Any) -> bool:
    if not isinstance(result, dict):
        return False
    return "expected a option<" in str(result.get("error", ""))


def _is_body_too_large(result: Any) -> bool:
    if not isinstance(result, dict):
        return False
    err = str(result.get("error", ""))
    if "413" in err:
        return True
    return any(sig in err for sig in _BODY_TOO_LARGE_SIGNALS)


__all__ = [
    "SurrealDBSink",
    "COVERAGE_COLUMNS",
    "SUFFIX_DISABLED",
]
