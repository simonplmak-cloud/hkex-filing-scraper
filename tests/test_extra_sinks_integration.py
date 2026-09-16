"""Live integration tests for the server-backed document, columnar, and graph sinks.

Each engine's tests are skipped unless that sink is configured, so the default
``pytest`` run stays offline. DuckDB runs in ``tests/test_duckdb_integration.py``.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from hkex_scraper import config, sinks

MONGO_READY = bool(config.MONGODB_URI and config.MONGODB_DATABASE)
CLICKHOUSE_READY = bool(config.CLICKHOUSE_HOST and config.CLICKHOUSE_DATABASE)
NEO4J_READY = bool(config.NEO4J_URI and config.NEO4J_USER and config.NEO4J_PASSWORD)


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


@pytest.mark.skipif(not MONGO_READY, reason="MongoDB not configured")
class TestMongoDBIntegration:
    def test_full_contract(self):
        sink = sinks.get_sink("mongodb")
        assert sink.available(), sink.unavailable_reason()
        ok, code = sink.ensure_schema()
        assert ok, code

        filing_id = _new_id()
        title = "mongo-it-" + filing_id
        assert sink.upsert_filings([_filing(filing_id, title)])[1] == ""
        assert sink.upsert_filings([_filing(filing_id, title)])[1] == ""
        rows, code = sink.fetch_filing_ids_by_ticker(["0451.HK"])
        assert code == ""
        assert sum(1 for r in rows if r.get("filing_id") == filing_id) == 1

        assert sink.upsert_document(filing_id, _doc())[0] is True
        assert sink.mark_status(filing_id, "processed")[0] is True
        assert (
            sink.upsert_edges(
                [{"company_ticker": "0451.HK", "filing_id": filing_id}], "has_filing"
            )[1]
            == ""
        )
        assert (
            sink.upsert_edges(
                [{"company_ticker": "0451.HK", "filing_id": filing_id}], "has_filing"
            )[0]
            == 0
        )
        assert sink.count_filings()[0] >= 1
        assert sink.count_edges("has_filing")[0] >= 1
        sink.close()


@pytest.mark.skipif(not CLICKHOUSE_READY, reason="ClickHouse not configured")
class TestClickHouseIntegration:
    def test_full_contract(self):
        sink = sinks.get_sink("clickhouse")
        assert sink.available(), sink.unavailable_reason()
        ok, code = sink.ensure_schema()
        assert ok, code

        filing_id = _new_id()
        title = "ch-it-" + filing_id
        assert sink.upsert_filings([_filing(filing_id, title)])[1] == ""
        # Re-write metadata: must not duplicate and must not lose the document.
        assert sink.upsert_document(filing_id, _doc())[0] is True
        assert sink.upsert_filings([_filing(filing_id, title)])[1] == ""

        rows, code = sink.fetch_filing_ids_by_ticker(["0451.HK"])
        assert code == ""
        assert sum(1 for r in rows if r.get("filing_id") == filing_id) == 1

        assert sink.mark_status(filing_id, "processed")[0] is True
        assert (
            sink.upsert_edges(
                [{"company_ticker": "0451.HK", "filing_id": filing_id}], "has_filing"
            )[1]
            == ""
        )
        assert sink.count_filings()[0] >= 1
        sink.close()


@pytest.mark.skipif(not NEO4J_READY, reason="Neo4j not configured")
class TestNeo4jIntegration:
    def test_full_contract(self):
        sink = sinks.get_sink("neo4j")
        assert sink.available(), sink.unavailable_reason()
        ok, code = sink.ensure_schema()
        assert ok, code

        filing_id = _new_id()
        title = "neo-it-" + filing_id
        assert sink.upsert_filings([_filing(filing_id, title)])[1] == ""
        # Re-run is idempotent (MERGE).
        assert sink.upsert_filings([_filing(filing_id, title)])[1] == ""

        rows, code = sink.fetch_filing_ids_by_ticker(["0451.HK"])
        assert code == ""
        assert sum(1 for r in rows if r.get("filing_id") == filing_id) == 1

        assert sink.upsert_document(filing_id, _doc())[0] is True
        assert sink.mark_status(filing_id, "processed")[0] is True
        created, code = sink.upsert_edges(
            [{"company_ticker": "0451.HK", "filing_id": filing_id}], "has_filing"
        )
        assert code == "" and created == 1
        # Idempotent edge: MERGE creates nothing the second time.
        created, code = sink.upsert_edges(
            [{"company_ticker": "0451.HK", "filing_id": filing_id}], "has_filing"
        )
        assert code == "" and created == 0
        assert sink.count_edges("has_filing")[0] >= 1
        sink.close()
