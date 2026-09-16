"""Integration tests for the PostgreSQL sink.

These run only when the optional driver is installed and a connection string is
configured (``POSTGRES_DSN`` or the discrete ``POSTGRES_*`` variables, in the
environment or the CWD ``.env``). Otherwise they are skipped, so the default
``pytest`` run stays offline.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from hkex_scraper import db_postgres

pytestmark = pytest.mark.skipif(
    not db_postgres.driver_installed() or not db_postgres.postgres_conninfo(),
    reason="PostgreSQL driver or connection string not configured",
)


@pytest.fixture(scope="module", autouse=True)
def _schema():
    ok, code = db_postgres.initialize_postgres_schema()
    assert ok, f"schema init failed: {code}"
    yield
    db_postgres.close_pool()


def _filing(filing_id: str) -> dict:
    return {
        "filing_id": filing_id,
        "company_ticker": "0451.HK",
        "stock_code": "0451",
        "stock_name": "Integration Test Holdings",
        "exchange": "HK",
        "filing_type": "Annual Report",
        "filing_subtype": "Annual Report",
        "filing_category": "LISTED_COMPANY",
        "title": "Integration test filing",
        "filing_date": datetime(2024, 7, 1, tzinfo=timezone.utc),
        "document_url": "https://example.invalid/doc.pdf",
        "referenced_tickers": ["0700.HK"],
        "source": "HKEx",
    }


def _doc() -> dict:
    return {
        "document_size": 12345,
        "document_type": "pdf",
        "document_hash": "deadbeef",
        "document_text": "# Integration\n\nbody text",
        "document_text_len": 25,
        "document_tables": [{"tableIndex": 0, "headers": ["A", "B"], "rowCount": 1}],
        "document_table_cnt": 1,
        "document_status": "processed",
        "document_status_reason": "",
    }


def _new_id() -> str:
    return "it" + uuid.uuid4().hex[:12]


def test_filing_upsert_is_idempotent_and_preserves_documents():
    filing_id = _new_id()
    filing = _filing(filing_id)

    written, code = db_postgres.upsert_filings([filing])
    assert code == "" and written == 1

    # Re-running must update, not duplicate.
    written, code = db_postgres.upsert_filings([filing])
    assert code == "" and written == 1
    count, code = db_postgres._fetch_scalar(
        "SELECT count(*) FROM exchange_filing WHERE filing_id = %s", [filing_id]
    )
    assert (count, code) == (1, "")

    # Document payload round-trips (JSONB + scalar columns).
    ok, code = db_postgres.upsert_document(filing_id, _doc())
    assert ok and code == ""

    # A metadata-only re-upsert must not clobber the document payload.
    db_postgres.upsert_filings([filing])
    rows, code = db_postgres._fetch_all(
        "SELECT document_text_len, jsonb_typeof(document_tables) AS tt, referenced_tickers "
        "FROM exchange_filing WHERE filing_id = %s",
        [filing_id],
    )
    assert code == ""
    assert rows[0]["document_text_len"] == 25
    assert rows[0]["tt"] == "array"
    assert rows[0]["referenced_tickers"] == ["0700.HK"]

    # A document write for a non-existent filing is a surfaced failure.
    ok, code = db_postgres.upsert_document("missing-" + filing_id, _doc())
    assert ok is False
    assert code == db_postgres.ERR_WRITE_ERROR


def test_edge_upsert_reports_created_excluding_conflicts():
    filing_id = _new_id()
    db_postgres.upsert_filings([_filing(filing_id)])

    created, code = db_postgres.upsert_edges(
        [{"company_id": "451_HK", "filing_id": filing_id}], "has_filing"
    )
    assert code == "" and created == 1

    # Idempotent: the conflict affects zero rows.
    created, code = db_postgres.upsert_edges(
        [{"company_id": "451_HK", "filing_id": filing_id}], "has_filing"
    )
    assert code == "" and created == 0

    rows, code = db_postgres._fetch_all(
        "SELECT count(*) AS n FROM has_filing WHERE filing_id = %s", [filing_id]
    )
    assert code == "" and rows[0]["n"] == 1

    total, code = db_postgres.count_edges("has_filing")
    assert code == ""
    assert total >= 1


def test_coverage_upsert_is_idempotent():
    run_id = "it-" + uuid.uuid4().hex[:8]
    chunk = {
        "chunk_from": datetime(2024, 7, 1, tzinfo=timezone.utc),
        "chunk_to": datetime(2024, 7, 31, tzinfo=timezone.utc),
        "api_count": 10,
        "ingested_count": 10,
        "unique_count": 9,
        "run_id": run_id,
    }
    ok, code = db_postgres.insert_coverage(chunk)
    assert ok and code == ""
    ok, code = db_postgres.insert_coverage(chunk)
    assert ok and code == ""

    rows, code = db_postgres._fetch_all(
        "SELECT count(*) AS n FROM scrape_coverage WHERE run_id = %s", [run_id]
    )
    assert code == "" and rows[0]["n"] == 1
