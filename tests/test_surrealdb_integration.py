"""Integration tests for the SurrealDB sink.

Run only when SurrealDB is configured (``DATABASE_TARGET`` includes surrealdb
and ``SURREAL_ENDPOINT`` / ``SURREAL_PASSWORD`` are set). Otherwise they are
skipped, so the default ``pytest`` run stays offline.
"""

from __future__ import annotations

import hashlib
import uuid

import pytest
from hkex_scraper import config, db, pipeline, sinks

pytestmark = pytest.mark.skipif(
    "surrealdb" not in config.sink_ids() or not config.SURREAL_ENDPOINT or not config.SURREAL_PASS,
    reason="SurrealDB not configured",
)


@pytest.fixture(scope="module", autouse=True)
def _schema():
    # A real run initialises every configured sink (main._init_schemas).
    for sink in sinks.enabled_sinks():
        ok, code = sink.ensure_schema()
        assert ok, f"{sink.id} schema init failed: {code}"
    yield
    sinks.close_all()


def _title() -> str:
    return "surreal-it-" + uuid.uuid4().hex[:10]


def _filing(title: str) -> dict:
    return {
        "stockCode": "0451",
        "date": "01/07/2024",
        "title": title,
        "stockName": "Surreal Integration Co",
        "link": "https://example.invalid/doc.pdf",
    }


def _filing_id(filing: dict) -> str:
    key = f"{filing['stockCode']}{filing['date']}{filing['title']}"
    return hashlib.md5(key.encode()).hexdigest()[:16]


def _count(sql: str) -> int:
    result = db.surreal_query(sql, timeout=30)
    assert isinstance(result, list) and result, result
    entry = result[0]
    assert entry.get("status") == "OK", entry
    rows = entry.get("result", [])
    return rows[0].get("count", 0) if rows else 0


def test_metadata_upsert_is_idempotent():
    title = _title()
    filing = _filing(title)
    assert pipeline._save_filings_batch_metadata([filing]) == 1
    assert pipeline._save_filings_batch_metadata([filing]) == 1
    assert _count(f"SELECT count() FROM exchange_filing WHERE title = '{title}' GROUP ALL;") == 1


def test_document_payload_is_saved():
    title = _title()
    filing = _filing(title)
    assert pipeline._save_filings_batch_metadata([filing]) == 1
    filing_id = _filing_id(filing)

    ok, code = pipeline._save_document_to_filing(
        filing_id,
        b"hello world",
        11,
        "https://example.invalid/doc.pdf",
        "extracted text",
        [{"tableIndex": 0, "headers": ["A"], "rowCount": 1}],
    )
    assert ok and code == ""

    result = db.surreal_query(
        f"SELECT documentStatus, documentTextLen, documentTableCnt "
        f"FROM exchange_filing:{filing_id};",
        timeout=30,
    )
    rows = result[0].get("result", []) if isinstance(result, list) else []
    assert rows, result
    row = rows[0]
    assert row.get("documentStatus") == "processed"
    assert row.get("documentTextLen") == len("extracted text")
    assert row.get("documentTableCnt") == 1


def test_coverage_row_is_written():
    run_id = "surreal-it-" + uuid.uuid4().hex[:8]
    db.surreal_query(
        "INSERT INTO scrape_coverage {"
        "  chunkFrom: d'2024-07-01', chunkTo: d'2024-07-31',"
        "  apiCount: 1, ingestedCount: 1, uniqueCount: 1,"
        f"  runId: '{run_id}', timestamp: time::now()"
        "};",
        timeout=30,
    )
    assert _count(f"SELECT count() FROM scrape_coverage WHERE runId = '{run_id}' GROUP ALL;") == 1


def test_graph_edge_is_created_when_company_table_configured():
    if not config.COMPANY_TABLE:
        pytest.skip("COMPANY_TABLE not configured")

    title = _title()
    filing = _filing(title)
    assert pipeline._save_filings_batch_metadata([filing]) == 1

    # Seed a matching company record using the configured id pattern.
    from hkex_scraper.utils import ticker_to_record_id

    company_id = ticker_to_record_id("0451.HK")
    db.surreal_query(f"CREATE {config.COMPANY_TABLE}:{company_id};", timeout=30)

    from hkex_scraper.graph import link_filings_to_companies

    link_filings_to_companies(ticker_set={"0451.HK"})

    edges = _count(
        f"SELECT count() FROM has_filing WHERE in = {config.COMPANY_TABLE}:{company_id} GROUP ALL;"
    )
    assert edges >= 1
