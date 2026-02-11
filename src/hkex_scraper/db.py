"""SurrealDB connection, schema initialisation, and query helpers."""

from __future__ import annotations

import base64
import json
import urllib.error
import urllib.request
from datetime import datetime
from typing import List

from .config import (
    COMPANY_TABLE,
    LOG_DIR,
    SURREAL_DB,
    SURREAL_ENDPOINT,
    SURREAL_NS,
    SURREAL_PASS,
    SURREAL_USER,
)
from .utils import log

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _get_auth_header() -> str:
    creds = f"{SURREAL_USER}:{SURREAL_PASS}".encode()
    return base64.b64encode(creds).decode()


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

def surreal_query(sql: str, timeout: int = 120) -> dict:
    """Send SurrealQL to the ``/sql`` endpoint. Returns parsed JSON response."""
    url = f"{SURREAL_ENDPOINT}/sql"
    auth = _get_auth_header()
    req = urllib.request.Request(
        url,
        data=sql.encode("utf-8"),
        headers={
            "Content-Type": "text/plain",
            "Accept": "application/json",
            "Authorization": f"Basic {auth}",
            "Surreal-NS": SURREAL_NS,
            "Surreal-DB": SURREAL_DB,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8") if e.fp else ""
        return {"error": f"HTTP {e.code}: {body[:500]}"}
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Batch upsert with binary-split retry
# ---------------------------------------------------------------------------

def upsert_batch_with_retry(
    statements: List[str], depth: int = 0, max_depth: int = 6
) -> int:
    """UPSERT a list of SurrealQL statements in one request.

    On error the batch is split in half and retried recursively.
    """
    if not statements:
        return 0
    batch_sql = "\n".join(statements)
    res = surreal_query(batch_sql, timeout=240)
    if isinstance(res, dict) and res.get("error"):
        if depth >= max_depth or len(statements) == 1:
            err_txt = res.get("error", "")
            log(f"  Batch failed (depth {depth}, size {len(statements)}): {err_txt[:300]}")
            try:
                LOG_DIR.mkdir(exist_ok=True)
                with open(LOG_DIR / "hkex_failed.sql", "a", encoding="utf-8") as fh:
                    fh.write(
                        "-- FAILED @ "
                        + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        + "\n"
                    )
                    fh.write("-- " + err_txt.replace("\n", " ")[:500] + "\n")
                    fh.write(statements[0] + "\n\n")
            except Exception:
                pass
            return 0
        mid = len(statements) // 2
        left = upsert_batch_with_retry(statements[:mid], depth + 1, max_depth)
        right = upsert_batch_with_retry(statements[mid:], depth + 1, max_depth)
        return left + right
    return len(statements)


# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------

def _build_schema_sql() -> str:
    """Return the full schema DDL, dynamically including graph edges if COMPANY_TABLE is set."""
    base = """
-- Filing table (SCHEMAFULL for type safety)
DEFINE TABLE IF NOT EXISTS exchange_filing SCHEMAFULL;

-- Metadata fields
DEFINE FIELD IF NOT EXISTS filingId       ON TABLE exchange_filing TYPE string;
DEFINE FIELD IF NOT EXISTS companyTicker  ON TABLE exchange_filing TYPE string;
DEFINE FIELD IF NOT EXISTS stockCode      ON TABLE exchange_filing TYPE string;
DEFINE FIELD IF NOT EXISTS stockName      ON TABLE exchange_filing TYPE option<string>;
DEFINE FIELD IF NOT EXISTS exchange       ON TABLE exchange_filing TYPE string;
DEFINE FIELD IF NOT EXISTS filingType     ON TABLE exchange_filing TYPE string;
DEFINE FIELD IF NOT EXISTS filingSubtype  ON TABLE exchange_filing TYPE option<string>;
DEFINE FIELD IF NOT EXISTS title          ON TABLE exchange_filing TYPE option<string>;
DEFINE FIELD IF NOT EXISTS filingDate     ON TABLE exchange_filing TYPE option<datetime>;
DEFINE FIELD IF NOT EXISTS documentUrl    ON TABLE exchange_filing TYPE option<string>;
DEFINE FIELD IF NOT EXISTS source         ON TABLE exchange_filing TYPE string;
DEFINE FIELD IF NOT EXISTS updatedAt      ON TABLE exchange_filing TYPE datetime;

-- Cross-reference fields
DEFINE FIELD IF NOT EXISTS referencedTickers ON TABLE exchange_filing TYPE option<array>;

-- Document fields (text + metadata only; raw blobs are NOT stored)
DEFINE FIELD IF NOT EXISTS documentSize         ON TABLE exchange_filing TYPE option<int>;
DEFINE FIELD IF NOT EXISTS documentType         ON TABLE exchange_filing TYPE option<string>;
DEFINE FIELD IF NOT EXISTS documentHash         ON TABLE exchange_filing TYPE option<string>;
DEFINE FIELD IF NOT EXISTS documentText         ON TABLE exchange_filing TYPE option<string>;
DEFINE FIELD IF NOT EXISTS documentTextLen      ON TABLE exchange_filing TYPE option<int>;
DEFINE FIELD IF NOT EXISTS documentTables       ON TABLE exchange_filing TYPE option<array>;
DEFINE FIELD IF NOT EXISTS documentTableCnt     ON TABLE exchange_filing TYPE option<int>;
DEFINE FIELD IF NOT EXISTS documentStatus       ON TABLE exchange_filing TYPE option<string>;
DEFINE FIELD IF NOT EXISTS documentStatusReason ON TABLE exchange_filing TYPE option<string>;

-- Indexes
DEFINE INDEX IF NOT EXISTS idx_ef_ticker    ON TABLE exchange_filing COLUMNS companyTicker;
DEFINE INDEX IF NOT EXISTS idx_ef_stockcode ON TABLE exchange_filing COLUMNS stockCode;
DEFINE INDEX IF NOT EXISTS idx_ef_date      ON TABLE exchange_filing COLUMNS filingDate;
DEFINE INDEX IF NOT EXISTS idx_ef_type      ON TABLE exchange_filing COLUMNS filingType;
DEFINE INDEX IF NOT EXISTS idx_ef_source    ON TABLE exchange_filing COLUMNS source;
DEFINE INDEX IF NOT EXISTS idx_ef_docstatus ON TABLE exchange_filing COLUMNS documentStatus;
"""
    if COMPANY_TABLE:
        base += f"""
-- Edge table: company -> filing (graph relation)
DEFINE TABLE IF NOT EXISTS has_filing SCHEMAFULL TYPE RELATION IN {COMPANY_TABLE} OUT exchange_filing;
DEFINE FIELD IF NOT EXISTS createdAt ON TABLE has_filing TYPE datetime;
DEFINE INDEX IF NOT EXISTS idx_hf_unique ON TABLE has_filing COLUMNS in, out UNIQUE;

-- Edge table: filing -> referenced company (graph relation)
DEFINE TABLE IF NOT EXISTS references_filing SCHEMAFULL TYPE RELATION IN exchange_filing OUT {COMPANY_TABLE};
DEFINE FIELD IF NOT EXISTS createdAt ON TABLE references_filing TYPE datetime;
DEFINE FIELD IF NOT EXISTS source    ON TABLE references_filing TYPE option<string>;
DEFINE INDEX IF NOT EXISTS idx_rf_unique ON TABLE references_filing COLUMNS in, out UNIQUE;
"""
    return base


def initialize_schema() -> bool:
    """Create tables, fields, indexes, and (optionally) edge tables.

    Idempotent — uses ``IF NOT EXISTS`` so it can be re-run safely.
    """
    schema_sql = _build_schema_sql()
    log("Initializing SurrealDB schema (SCHEMAFULL + indexes)...")
    result = surreal_query(schema_sql, timeout=60)
    if isinstance(result, dict) and result.get("error"):
        log(f"  Schema init warning: {result['error'][:300]}")
        return False
    log("  Schema initialized successfully")
    if COMPANY_TABLE:
        log(f"  Graph edges enabled (company table: {COMPANY_TABLE})")
    else:
        log("  Graph edges disabled (set COMPANY_TABLE to enable)")

    # Migration: remove deprecated documentContent field
    mig_result = surreal_query(
        "REMOVE FIELD IF EXISTS documentContent ON TABLE exchange_filing;",
        timeout=30,
    )
    if isinstance(mig_result, dict) and mig_result.get("error"):
        log(
            f"  Migration note: could not remove documentContent field: "
            f"{mig_result['error'][:200]}"
        )
    else:
        log("  Migration: documentContent field removed (no longer storing blobs)")
    return True
