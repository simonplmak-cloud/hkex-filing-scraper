"""In-process SQLite contract tests: the full sink write/read surface.

SQLite needs no service, so these tests always run and exercise the shared
relational engine and dialect end to end.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from hkex_scraper.sinks.sqlite import SQLiteSink


@pytest.fixture()
def sink(tmp_path, monkeypatch):
    from hkex_scraper.sinks import sqlite as sqlite_module

    path = tmp_path / "hkex-test.db"
    monkeypatch.setattr(sqlite_module.config, "SQLITE_PATH", str(path))
    s = SQLiteSink()
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


def test_filing_upsert_is_idempotent_and_json_round_trips(sink):
    record = _filing("f1")
    written, code = sink.upsert_filings([record])
    assert (written, code) == (1, "")

    # Re-run updates rather than duplicating.
    written, code = sink.upsert_filings([record])
    assert (written, code) == (1, "")

    count, code = sink.count_filings()
    assert (count, code) == (1, "")

    rows, code = sink._read(
        "SELECT referenced_tickers FROM exchange_filing WHERE filing_id = ?", ["f1"]
    )
    assert code == ""
    assert json.loads(rows[0]["referenced_tickers"]) == ["0700.HK"]


def test_metadata_upsert_does_not_clobber_document(sink):
    sink.upsert_filings([_filing("f2")])
    ok, code = sink.upsert_document("f2", _doc("hello world"))
    assert ok and code == ""

    # Metadata-only re-upsert must leave the document payload untouched.
    sink.upsert_filings([_filing("f2")])
    rows, code = sink._read(
        "SELECT document_text, document_text_len FROM exchange_filing WHERE filing_id = ?", ["f2"]
    )
    assert code == ""
    assert rows[0]["document_text"] == "hello world"
    assert rows[0]["document_text_len"] == len("hello world")


def test_document_write_for_missing_filing_is_a_failure(sink):
    ok, code = sink.upsert_document("missing", _doc())
    assert ok is False
    assert code == "SQLITE_WRITE_ERROR"


def test_mark_status(sink):
    sink.upsert_filings([_filing("f3")])
    ok, code = sink.mark_status("f3", "skipped", "no_document_url")
    assert ok and code == ""
    rows, _ = sink._read(
        "SELECT document_status, document_status_reason FROM exchange_filing WHERE filing_id = ?",
        ["f3"],
    )
    assert rows[0]["document_status"] == "skipped"
    assert rows[0]["document_status_reason"] == "no_document_url"


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
    assert records[0]["run_id"] == "run-1"
    assert records[0]["unique_count"] == 9


def test_edges_are_conflict_noops(sink):
    sink.upsert_filings([_filing("f4")])

    created, code = sink.upsert_edges(
        [{"company_ticker": "0451.HK", "filing_id": "f4"}], "has_filing"
    )
    assert (created, code) == (1, "")

    # Idempotent: conflict affects zero rows.
    created, code = sink.upsert_edges(
        [{"company_ticker": "0451.HK", "filing_id": "f4"}], "has_filing"
    )
    assert (created, code) == (0, "")

    created, code = sink.upsert_edges(
        [{"filing_id": "f4", "company_ticker": "0700.HK"}], "references_filing"
    )
    assert (created, code) == (1, "")

    total, code = sink.count_edges("has_filing")
    assert (total, code) == (1, "")
    total, code = sink.count_edges("references_filing")
    assert (total, code) == (1, "")


def test_read_routing_surface(sink):
    sink.upsert_filings(
        [
            _filing("p1", ticker="0451.HK", title="Annual Report 2024"),
            _filing("p2", ticker="0700.HK", title="Interim Results 2024"),
        ]
    )
    sink.upsert_document("p1", _doc())

    pending, code = sink.count_pending_filings()
    assert code == "" and pending == 1

    rows, code = sink.fetch_pending_filings(10)
    assert code == "" and [r["filing_id"] for r in rows] == ["p2"]

    tickers, code = sink.distinct_company_tickers()
    assert code == "" and sorted(tickers) == ["0451.HK", "0700.HK"]

    pairs, code = sink.fetch_filing_ids_by_ticker(["0451.HK"])
    assert code == "" and pairs == [{"company_ticker": "0451.HK", "filing_id": "p1"}]

    titles, code = sink.fetch_titles(None, 0, 10)
    assert code == "" and len(titles) == 2
    assert {"filing_id", "title", "stock_code", "company_ticker"} <= set(titles[0])

    subset, code = sink.fetch_titles(["0700.HK"], 0, 10)
    assert code == "" and [t["filing_id"] for t in subset] == ["p2"]


def test_document_tables_round_trip_as_json(sink):
    sink.upsert_filings([_filing("f5")])
    sink.upsert_document("f5", _doc())
    rows, code = sink._read(
        "SELECT document_tables, document_table_cnt FROM exchange_filing WHERE filing_id = ?",
        ["f5"],
    )
    assert code == ""
    tables = json.loads(rows[0]["document_tables"])
    assert tables[0]["tableIndex"] == 0
    assert rows[0]["document_table_cnt"] == 1
