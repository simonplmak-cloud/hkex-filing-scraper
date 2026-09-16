"""Multi-sink parity test: requires two or more configured sinks.

Run only when ``DATABASE_TARGET`` includes both sinks and both are reachable.
Otherwise skipped.
"""

from __future__ import annotations

import hashlib
import uuid

import pytest

from hkex_scraper import config, db, db_postgres, pipeline, sinks

pytestmark = pytest.mark.skipif(
    not (
        "surrealdb" in config.sink_ids()
        and "postgres" in config.sink_ids()
        and config.SURREAL_ENDPOINT
        and config.SURREAL_PASS
        and db_postgres.postgres_available()
    ),
    reason="Both SurrealDB and PostgreSQL must be configured",
)


@pytest.fixture(scope="module", autouse=True)
def _schema():
    # A real run initialises every configured sink (main._init_schemas).
    for sink in sinks.enabled_sinks():
        ok, code = sink.ensure_schema()
        assert ok, f"{sink.id} schema init failed: {code}"
    yield
    sinks.close_all()


def _surreal_count(where: str) -> int:
    result = db.surreal_query(f"SELECT count() FROM exchange_filing WHERE {where} GROUP ALL;", 30)
    entry = result[0] if isinstance(result, list) and result else {}
    rows = entry.get("result", []) if isinstance(entry, dict) else []
    return rows[0].get("count", 0) if rows else 0


def test_same_filing_lands_in_both_sinks():
    title = "dual-it-" + uuid.uuid4().hex[:10]
    filing = {
        "stockCode": "0005",
        "date": "01/07/2024",
        "title": title,
        "stockName": "Multi Sink Co",
        "link": "https://example.invalid/doc.pdf",
    }
    assert pipeline._save_filings_batch_metadata([filing]) == 1

    assert _surreal_count(f"title = '{title}'") == 1
    rows, code = db_postgres._fetch_all(
        "SELECT count(*) AS n FROM exchange_filing WHERE title = %s", [title]
    )
    assert code == "" and rows[0]["n"] == 1


def test_document_status_mirrors_across_sinks():
    title = "dual-it-" + uuid.uuid4().hex[:10]
    filing = {
        "stockCode": "0005",
        "date": "01/07/2024",
        "title": title,
        "stockName": "Multi Sink Co",
        "link": "https://example.invalid/doc.pdf",
    }
    pipeline._save_filings_batch_metadata([filing])
    filing_id = hashlib.md5(
        f"{filing['stockCode']}{filing['date']}{filing['title']}".encode()
    ).hexdigest()[:16]

    ok, code = pipeline._save_document_to_filing(
        filing_id, b"body", 4, "https://example.invalid/doc.pdf", "text", []
    )
    assert ok and code == ""

    result = db.surreal_query(
        f"SELECT documentStatus FROM exchange_filing:{filing_id};", timeout=30
    )
    rows = result[0].get("result", []) if isinstance(result, list) else []
    assert rows and rows[0].get("documentStatus") == "processed"

    pg_rows, pg_code = db_postgres._fetch_all(
        "SELECT document_status FROM exchange_filing WHERE filing_id = %s", [filing_id]
    )
    assert pg_code == ""
    assert pg_rows[0]["document_status"] == "processed"
