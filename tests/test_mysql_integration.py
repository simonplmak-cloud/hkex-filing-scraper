"""Live integration tests for the MySQL and MariaDB sinks.

Skipped unless the relevant connection variables are set (``MYSQL_*`` and/or
``MARIADB_*``), so the default ``pytest`` run stays offline.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from hkex_scraper import config, sinks

MYSQL_READY = bool(config.mysql_conn_kwargs("MYSQL"))
MARIADB_READY = bool(config.mysql_conn_kwargs("MARIADB"))


def _filing(filing_id: str, title: str) -> dict:
    return {
        "filing_id": filing_id,
        "company_ticker": "0451.HK",
        "stock_code": "0451",
        "stock_name": "Integration Co",
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


def _doc(text: str = "integration text") -> dict:
    return {
        "document_size": 16,
        "document_type": "pdf",
        "document_hash": "deadbeef",
        "document_text": text,
        "document_text_len": len(text),
        "document_tables": [{"tableIndex": 0, "headers": ["A"], "rowCount": 1}],
        "document_table_cnt": 1,
        "document_status": "processed",
        "document_status_reason": "",
    }


def _new_id() -> str:
    return "it" + uuid.uuid4().hex[:12]


def _exercise(sink_id: str) -> None:
    sink = sinks.get_sink(sink_id)
    assert sink.available(), sink.unavailable_reason()
    ok, code = sink.ensure_schema()
    assert ok, code
    try:
        filing_id = _new_id()
        title = f"{sink_id}-it-" + filing_id

        # Idempotent metadata upsert.
        assert sink.upsert_filings([_filing(filing_id, title)])[1] == ""
        assert sink.upsert_filings([_filing(filing_id, title)])[1] == ""

        rows, code = sink.fetch_filing_ids_by_ticker(["0451.HK"])
        assert code == ""
        assert sum(1 for r in rows if r.get("filing_id") == filing_id) == 1

        # Document write, then a metadata re-write must not clobber it.
        assert sink.upsert_document(filing_id, _doc())[0] is True
        assert sink.upsert_filings([_filing(filing_id, title)])[1] == ""

        # Edges: first insert creates, second is a conflict no-op (INSERT IGNORE).
        created, code = sink.upsert_edges(
            [{"company_ticker": "0451.HK", "filing_id": filing_id}], "has_filing"
        )
        assert code == "" and created == 1, f"first edge create: {(created, code)}"
        created, code = sink.upsert_edges(
            [{"company_ticker": "0451.HK", "filing_id": filing_id}], "has_filing"
        )
        assert code == "" and created == 0, f"idempotent edge: {(created, code)}"

        assert sink.mark_status(filing_id, "processed")[0] is True
        assert sink.count_filings()[0] >= 1
        assert sink.count_edges("has_filing")[0] >= 1
    finally:
        sink.close()


@pytest.mark.skipif(not MYSQL_READY, reason="MySQL not configured")
class TestMySQLIntegration:
    def test_full_contract(self):
        _exercise("mysql")


@pytest.mark.skipif(not MARIADB_READY, reason="MariaDB not configured")
class TestMariaDBIntegration:
    def test_full_contract(self):
        _exercise("mariadb")
