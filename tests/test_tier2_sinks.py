"""Unit tests for the Tier 2 sinks: MongoDB, ClickHouse, Neo4j.

Pure unit tests — no database required. Driver-missing/unconfigured degradation,
capability declarations, and query builders are asserted directly, and the
ClickHouse read-merge path is exercised with the client monkeypatched.
"""

from __future__ import annotations

from datetime import datetime, timezone

from hkex_scraper import config
from hkex_scraper.sinks import clickhouse as ch_module
from hkex_scraper.sinks import mongodb as mongo_module
from hkex_scraper.sinks import neo4j as neo4j_module


# ---------------------------------------------------------------------------
# Capabilities
# ---------------------------------------------------------------------------


class TestCapabilities:
    def test_mongodb_is_a_document_sink(self):
        caps = mongo_module.MongoDBSink().capabilities
        assert caps.model == "document"
        assert caps.reads and caps.edges and caps.arrays

    def test_clickhouse_declares_no_native_upsert(self):
        caps = ch_module.ClickHouseSink().capabilities
        assert caps.model == "columnar"
        assert caps.native_upsert is False

    def test_neo4j_is_a_graph_sink(self):
        caps = neo4j_module.Neo4jSink().capabilities
        assert caps.model == "graph"
        assert caps.reads and caps.edges


# ---------------------------------------------------------------------------
# Graceful degradation
# ---------------------------------------------------------------------------


class TestMongoDegradation:
    def test_driver_missing(self, monkeypatch):
        monkeypatch.setattr(mongo_module, "_PYMONGO_AVAILABLE", False)
        sink = mongo_module.MongoDBSink()
        assert sink.available() is False
        assert "pymongo" in sink.unavailable_reason()
        assert sink.upsert_filings([{"filing_id": "x"}]) == (0, "MONGODB_DRIVER_MISSING")
        ok, code = sink.upsert_document("x", {})
        assert ok is False and code == "MONGODB_DRIVER_MISSING"

    def test_unconfigured(self, monkeypatch):
        monkeypatch.setattr(mongo_module, "_PYMONGO_AVAILABLE", True)
        monkeypatch.setattr(config, "MONGODB_URI", "")
        monkeypatch.setattr(config, "MONGODB_DATABASE", "")
        sink = mongo_module.MongoDBSink()
        assert sink.available() is False
        assert "MONGODB_URI" in sink.unavailable_reason()


class TestClickHouseDegradation:
    def test_driver_missing(self, monkeypatch):
        monkeypatch.setattr(ch_module, "_CLICKHOUSE_AVAILABLE", False)
        sink = ch_module.ClickHouseSink()
        assert sink.available() is False
        assert "clickhouse-connect" in sink.unavailable_reason()
        assert sink.upsert_filings([{"filing_id": "x"}]) == (0, "CLICKHOUSE_DRIVER_MISSING")

    def test_unconfigured(self, monkeypatch):
        monkeypatch.setattr(ch_module, "_CLICKHOUSE_AVAILABLE", True)
        monkeypatch.setattr(config, "CLICKHOUSE_HOST", "")
        monkeypatch.setattr(config, "CLICKHOUSE_DATABASE", "")
        sink = ch_module.ClickHouseSink()
        assert sink.available() is False
        assert "CLICKHOUSE_HOST" in sink.unavailable_reason()


class TestNeo4jDegradation:
    def test_driver_missing(self, monkeypatch):
        monkeypatch.setattr(neo4j_module, "_NEO4J_AVAILABLE", False)
        sink = neo4j_module.Neo4jSink()
        assert sink.available() is False
        assert "neo4j" in sink.unavailable_reason()
        assert sink.upsert_filings([{"filing_id": "x"}]) == (0, "NEO4J_DRIVER_MISSING")

    def test_unconfigured(self, monkeypatch):
        monkeypatch.setattr(neo4j_module, "_NEO4J_AVAILABLE", True)
        monkeypatch.setattr(config, "NEO4J_URI", "")
        monkeypatch.setattr(config, "NEO4J_USER", "")
        monkeypatch.setattr(config, "NEO4J_PASSWORD", "")
        sink = neo4j_module.Neo4jSink()
        assert sink.available() is False
        assert "NEO4J_URI" in sink.unavailable_reason()


# ---------------------------------------------------------------------------
# ClickHouse: DDL and read-merge behaviour
# ---------------------------------------------------------------------------


class TestClickHouseSchema:
    def test_ddl_uses_replacing_merge_tree(self):
        joined = "\n".join(ch_module._DDL)
        for table in ("exchange_filing", "scrape_coverage", "has_filing", "references_filing"):
            assert f"CREATE TABLE IF NOT EXISTS {table}" in joined
        assert "ReplacingMergeTree" in joined
        assert "ORDER BY filing_id" in joined
        assert "Array(String)" in joined


class TestClickHouseMerge:
    def test_metadata_merge_preserves_document_columns(self, monkeypatch):
        sink = ch_module.ClickHouseSink()
        monkeypatch.setattr(
            sink,
            "_query",
            lambda sql, params=None: (
                [
                    {
                        "filing_id": "f1",
                        "document_size": 11,
                        "document_text": "kept",
                        "document_status": "processed",
                    }
                ],
                "",
            ),
        )
        captured = {}

        def fake_insert(table, rows, columns):
            captured["rows"] = rows
            return True, ""

        monkeypatch.setattr(sink, "_insert", fake_insert)
        written, code = sink.upsert_filings(
            [
                {
                    "filing_id": "f1",
                    "company_ticker": "0451.HK",
                    "stock_code": "0451",
                    "filing_type": "Annual Report",
                    "referenced_tickers": ["0700.HK"],
                    "updated_at": datetime.now(timezone.utc),
                }
            ]
        )
        assert (written, code) == (1, "")
        row = dict(zip(ch_module._FILING_COLUMNS, captured["rows"][0]))
        # Document columns survive the metadata write.
        assert row["document_text"] == "kept"
        assert row["document_status"] == "processed"
        # Metadata is applied.
        assert row["company_ticker"] == "0451.HK"

    def test_document_write_for_missing_row_is_failure(self, monkeypatch):
        sink = ch_module.ClickHouseSink()
        monkeypatch.setattr(sink, "_fetch_filing", lambda fid: (None, ""))
        ok, code = sink.upsert_document("missing", {"document_status": "processed"})
        assert ok is False and code == "CLICKHOUSE_WRITE_ERROR"

    def test_document_merge_preserves_metadata(self, monkeypatch):
        sink = ch_module.ClickHouseSink()
        existing = {
            "filing_id": "f1",
            "company_ticker": "0451.HK",
            "document_text": None,
        }
        monkeypatch.setattr(sink, "_fetch_filing", lambda fid: (dict(existing), ""))
        captured = {}

        def fake_insert(table, rows, columns):
            captured["row"] = dict(zip(columns, rows[0]))
            return True, ""

        monkeypatch.setattr(sink, "_insert", fake_insert)
        ok, code = sink.upsert_document(
            "f1", {"document_text": "new", "document_status": "processed"}
        )
        assert ok and code == ""
        assert captured["row"]["company_ticker"] == "0451.HK"
        assert captured["row"]["document_text"] == "new"


# ---------------------------------------------------------------------------
# Neo4j: Cypher shape
# ---------------------------------------------------------------------------


class TestNeo4jCypher:
    def test_constraints_are_idempotent(self):
        joined = "\n".join(neo4j_module._CONSTRAINTS)
        assert "IF NOT EXISTS" in joined
        assert "Filing" in joined and "Company" in joined

    def test_edges_use_merge(self):
        for kind in ("has_filing", "references_filing"):
            query = neo4j_module._UPSERT_EDGE[kind]
            assert "MERGE" in query
            assert "$edges" in query

    def test_document_write_for_missing_node_is_failure(self, monkeypatch):
        sink = neo4j_module.Neo4jSink()
        monkeypatch.setattr(sink, "_run", lambda query, params=None: ([{"n": 0}], None, ""))
        ok, code = sink.upsert_document("missing", {"document_status": "processed"})
        assert ok is False and code == "NEO4J_WRITE_ERROR"

    def test_upsert_filings_counts_records(self, monkeypatch):
        sink = neo4j_module.Neo4jSink()
        monkeypatch.setattr(sink, "_run", lambda query, params=None: ([], None, ""))
        written, code = sink.upsert_filings([{"filing_id": "f1"}, {"filing_id": "f2"}])
        assert (written, code) == (2, "")
