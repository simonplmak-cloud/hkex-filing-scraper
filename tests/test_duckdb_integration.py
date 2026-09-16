"""In-process DuckDB contract tests: the full sink write/read surface.

Skipped when the optional ``duckdb`` driver is not installed. DuckDB runs
in-process, so these tests need no service.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

pytest.importorskip("duckdb")

from hkex_scraper import config  # noqa: E402
from hkex_scraper.sinks.duckdb import DuckDBSink  # noqa: E402


@pytest.fixture()
def sink(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DUCKDB_PATH", str(tmp_path / "hkex-test.duckdb"))
    s = DuckDBSink()
    assert s.available() is True
    ok, code = s.ensure_schema()
    assert ok, code
    yield s
    s.close()


def _filing(filing_id: str, ticker: str = "0451.HK", title: str = "Annual Report 2024") -> dict:
    return {
        "filing_id": filing_id,
        "company_ticker": ticker,
        "stock_code": ticker.split(".")[0],
        "stock_name": "Example Holdings",
        "exchange": "HK",
        "filing_type": "Annual Report",
        "filing_subtype": "Annual Report",
        "filing_category": "LISTED_COMPANY",
        "title": title,
        "filing_date": datetime(2024, 7, 1, tzinfo=timezone.utc),
        "document_url": "https://example.invalid/doc.pdf",
        "referenced_tickers": ["0700.HK"],
        "source": "HKEx",
        "updated_at": datetime.now(timezone.utc),
    }


def _doc(text: str = "extracted body text") -> dict:
    return {
        "document_size": 11,
        "document_type": "pdf",
        "document_hash": "deadbeef",
        "document_text": text,
        "document_text_len": len(text),
        "document_tables": [{"tableIndex": 0, "headers": ["A"], "rowCount": 1}],
        "document_table_cnt": 1,
        "document_status": "processed",
        "document_status_reason": "",
    }


def test_schema_is_idempotent(sink):
    ok, code = sink.ensure_schema()
    assert ok, code


def test_filing_upsert_is_idempotent(sink):
    record = _filing("f1")
    assert sink.upsert_filings([record]) == (1, "")
    assert sink.upsert_filings([record]) == (1, "")
    assert sink.count_filings() == (1, "")


def test_metadata_upsert_does_not_clobber_document(sink):
    sink.upsert_filings([_filing("f2")])
    ok, code = sink.upsert_document("f2", _doc("hello world"))
    assert ok and code == ""
    sink.upsert_filings([_filing("f2")])
    rows, code = sink._read(
        'SELECT document_text FROM "exchange_filing" WHERE filing_id = ?', ["f2"]
    )
    assert code == ""
    assert rows[0]["document_text"] == "hello world"


def test_document_write_for_missing_filing_is_a_failure(sink):
    ok, code = sink.upsert_document("missing", _doc())
    assert ok is False
    assert code == "DUCKDB_WRITE_ERROR"


def test_mark_status(sink):
    sink.upsert_filings([_filing("f3")])
    assert sink.mark_status("f3", "skipped", "no_document_url") == (True, "")
    rows, _ = sink._read(
        'SELECT document_status FROM "exchange_filing" WHERE filing_id = ?', ["f3"]
    )
    assert rows[0]["document_status"] == "skipped"


def test_coverage_upsert_is_idempotent(sink):
    chunk = {
        "chunk_from": datetime(2024, 7, 1, tzinfo=timezone.utc),
        "chunk_to": datetime(2024, 7, 31, tzinfo=timezone.utc),
        "api_count": 10,
        "ingested_count": 10,
        "unique_count": 9,
        "run_id": "run-1",
        "timestamp": datetime.now(timezone.utc),
    }
    assert sink.insert_coverage(chunk) == (True, "")
    assert sink.insert_coverage(chunk) == (True, "")
    records, code = sink.fetch_coverage()
    assert code == "" and len(records) == 1


def test_edges_are_conflict_noops(sink):
    sink.upsert_filings([_filing("f4")])
    created, code = sink.upsert_edges(
        [{"company_ticker": "0451.HK", "filing_id": "f4"}], "has_filing"
    )
    assert (created, code) == (1, "")
    created, code = sink.upsert_edges(
        [{"company_ticker": "0451.HK", "filing_id": "f4"}], "has_filing"
    )
    assert (created, code) == (0, "")
    assert sink.count_edges("has_filing") == (1, "")


def test_read_routing_surface(sink):
    sink.upsert_filings(
        [
            _filing("p1", ticker="0451.HK", title="Annual Report 2024"),
            _filing("p2", ticker="0700.HK", title="Interim Results 2024"),
        ]
    )
    sink.upsert_document("p1", _doc())

    assert sink.count_pending_filings() == (1, "")
    rows, code = sink.fetch_pending_filings(10)
    assert code == "" and [r["filing_id"] for r in rows] == ["p2"]

    tickers, code = sink.distinct_company_tickers()
    assert code == "" and sorted(tickers) == ["0451.HK", "0700.HK"]

    pairs, code = sink.fetch_filing_ids_by_ticker(["0451.HK"])
    assert code == "" and pairs == [{"company_ticker": "0451.HK", "filing_id": "p1"}]

    titles, code = sink.fetch_titles(None, 0, 10)
    assert code == "" and len(titles) == 2


def test_document_tables_round_trip_as_json(sink):
    sink.upsert_filings([_filing("f5")])
    sink.upsert_document("f5", _doc())
    rows, code = sink._read(
        'SELECT document_tables FROM "exchange_filing" WHERE filing_id = ?', ["f5"]
    )
    assert code == ""
    tables = json.loads(rows[0]["document_tables"])
    assert tables[0]["tableIndex"] == 0
