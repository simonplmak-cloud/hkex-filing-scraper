"""Conformance tests for the read-only surfaces added for the MCP server.

Every relational dialect must return the same canonical filing-detail shape, and the
title filter must behave identically. SQLite is stdlib and always runs; DuckDB is
exercised when the optional driver is installed. The document and graph sinks are
checked through their query builders with the driver monkeypatched (no service).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from hkex_scraper import config
from hkex_scraper.sinks.sqlite import SQLiteSink

# The canonical detail contract every sink must satisfy.
DETAIL_KEYS = {
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
    "document_size",
    "document_type",
    "document_hash",
    "document_sha256",
    "document_text",
    "document_text_len",
    "document_tables",
    "document_table_cnt",
    "document_status",
    "document_status_reason",
}


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
        "document_sha256": "a" * 64,
        "document_text": text,
        "document_text_len": len(text),
        "document_tables": [{"tableIndex": 0, "headers": ["A"], "rowCount": 1}],
        "document_table_cnt": 1,
        "document_status": "processed",
        "document_status_reason": "",
    }


@pytest.fixture(params=["sqlite", "duckdb"])
def sink(request, tmp_path, monkeypatch):
    """A ready relational sink, parametrized across the in-process engines."""
    if request.param == "duckdb":
        pytest.importorskip("duckdb")
        from hkex_scraper.sinks.duckdb import DuckDBSink

        monkeypatch.setattr(config, "DUCKDB_PATH", str(tmp_path / "hkex.duckdb"))
        s = DuckDBSink()
    else:
        monkeypatch.setattr(config, "SQLITE_PATH", str(tmp_path / "hkex.db"))
        s = SQLiteSink()
    assert s.available() is True
    ok, code = s.ensure_schema()
    assert ok, code
    yield s
    s.close()


# ---------------------------------------------------------------------------
# fetch_filing_detail
# ---------------------------------------------------------------------------
def test_detail_has_the_canonical_shape(sink):
    sink.upsert_filings([_filing("f1")])
    assert sink.upsert_document("f1", _doc("hello world")) == (True, "")

    detail, code = sink.fetch_filing_detail("f1")
    assert code == ""
    assert detail is not None
    assert DETAIL_KEYS <= set(detail)
    assert detail["filing_id"] == "f1"
    assert detail["company_ticker"] == "0451.HK"
    assert detail["document_text"] == "hello world"
    # JSON-text columns are decoded for the read consumer.
    assert detail["referenced_tickers"] == ["0700.HK"]
    assert detail["document_tables"][0]["tableIndex"] == 0


def test_detail_for_an_unknown_id_is_not_an_error(sink):
    detail, code = sink.fetch_filing_detail("does-not-exist")
    assert (detail, code) == (None, "")


def test_detail_without_a_document_still_returns_metadata(sink):
    sink.upsert_filings([_filing("f2")])
    detail, code = sink.fetch_filing_detail("f2")
    assert code == "" and detail is not None
    assert detail["filing_id"] == "f2"
    assert detail["document_status"] is None
    assert detail["document_text"] is None


# ---------------------------------------------------------------------------
# title search (fetch_titles title_query)
# ---------------------------------------------------------------------------
def test_title_search_is_a_case_insensitive_substring(sink):
    sink.upsert_filings(
        [
            _filing("t1", title="Annual Report 2024"),
            _filing("t2", title="Interim Results 2024"),
            _filing("t3", ticker="0700.HK", title="Monthly Return"),
        ]
    )
    rows, code = sink.fetch_titles(None, 0, 10, "annual")
    assert code == ""
    assert [r["filing_id"] for r in rows] == ["t1"]

    rows, code = sink.fetch_titles(None, 0, 10, "RESULTS")
    assert code == "" and [r["filing_id"] for r in rows] == ["t2"]


def test_title_search_combines_with_a_ticker_filter(sink):
    sink.upsert_filings(
        [
            _filing("c1", ticker="0451.HK", title="Annual Report"),
            _filing("c2", ticker="0700.HK", title="Annual Report"),
        ]
    )
    rows, code = sink.fetch_titles(["0700.HK"], 0, 10, "annual")
    assert code == "" and [r["filing_id"] for r in rows] == ["c2"]

    # No query => the original ticker-only behaviour is preserved.
    rows, code = sink.fetch_titles(["0700.HK"], 0, 10)
    assert code == "" and [r["filing_id"] for r in rows] == ["c2"]


def test_title_search_pages(sink):
    sink.upsert_filings([_filing(f"p{i}", title=f"Annual Report {i}") for i in range(5)])
    first, code = sink.fetch_titles(None, 0, 2, "annual")
    assert code == "" and len(first) == 2
    second, code = sink.fetch_titles(None, 2, 2, "annual")
    assert code == "" and len(second) == 2
    assert {r["filing_id"] for r in first}.isdisjoint({r["filing_id"] for r in second})


# ---------------------------------------------------------------------------
# Dialect SQL builders
# ---------------------------------------------------------------------------
def _dialects():
    from hkex_scraper.sinks.dialects import (
        DuckDBDialect,
        MySQLDialect,
        PostgresDialect,
        SQLiteDialect,
    )

    return [PostgresDialect(), MySQLDialect("mysql"), SQLiteDialect(), DuckDBDialect()]


class TestDialectSql:
    @pytest.mark.parametrize("dialect", _dialects(), ids=lambda d: d.id)
    def test_detail_sql_selects_all_columns_by_id(self, dialect):
        sql = dialect.fetch_filing_detail_sql()
        assert "WHERE" in sql and "LIMIT 1" in sql
        assert dialect.q("filing_id") in sql
        for column in ("document_text", "document_tables", "referenced_tickers"):
            assert dialect.q(column) in sql

    @pytest.mark.parametrize("dialect", _dialects(), ids=lambda d: d.id)
    def test_title_sql_adds_a_case_insensitive_filter(self, dialect):
        sql = dialect.fetch_titles_sql(False, with_query=True)
        assert "LOWER(" in sql and "LIKE LOWER(" in sql
        plain = dialect.fetch_titles_sql(False, with_query=False)
        assert "LIKE" not in plain

    def test_decode_row_parses_json_text_and_leaves_native_values(self):
        from hkex_scraper.sinks.dialects import decode_row

        decoded = decode_row(
            {
                "referenced_tickers": '["0700.HK"]',
                "document_tables": '[{"tableIndex": 0}]',
                "document_text": "kept",
                "document_status": "",
            }
        )
        assert decoded["referenced_tickers"] == ["0700.HK"]
        assert decoded["document_tables"] == [{"tableIndex": 0}]
        # Non-JSON columns are left untouched.
        assert decoded["document_text"] == "kept"
        assert decoded["document_status"] == ""

        native = decode_row({"referenced_tickers": ["0045.HK"], "document_tables": [{"a": 1}]})
        assert native["referenced_tickers"] == ["0045.HK"]
        assert native["document_tables"] == [{"a": 1}]


# ---------------------------------------------------------------------------
# Document and graph sinks (driver monkeypatched; no service)
# ---------------------------------------------------------------------------
class TestMongoDetail:
    def test_maps_document_and_drops_internal_id(self, monkeypatch):
        from hkex_scraper.sinks import mongodb as mongo

        class _Collection:
            def find_one(self, query, projection):
                assert query == {"_id": "f1"}
                assert "document_text" in projection and "_id" not in projection
                return {"_id": "f1", "title": "Report", "document_text": "body"}

        class _DB:
            def __getitem__(self, name):
                return _Collection()

        sink = mongo.MongoDBSink()
        monkeypatch.setattr(sink, "_database", lambda: (_DB(), ""))
        detail, code = sink.fetch_filing_detail("f1")
        assert code == ""
        assert detail is not None
        assert detail["filing_id"] == "f1"
        assert detail["document_text"] == "body"
        assert "_id" not in detail

    def test_not_found_is_none(self, monkeypatch):
        from hkex_scraper.sinks import mongodb as mongo

        class _Collection:
            def find_one(self, query, projection):
                return None

        class _DB:
            def __getitem__(self, name):
                return _Collection()

        sink = mongo.MongoDBSink()
        monkeypatch.setattr(sink, "_database", lambda: (_DB(), ""))
        assert sink.fetch_filing_detail("nope") == (None, "")


class TestClickHouseDetail:
    def test_decodes_json_text_columns(self, monkeypatch):
        from hkex_scraper.sinks import clickhouse as ch

        sink = ch.ClickHouseSink()
        monkeypatch.setattr(
            sink,
            "_query",
            lambda sql, params=None: (
                [
                    {
                        "filing_id": "f1",
                        "document_tables": '[{"tableIndex": 0}]',
                        "referenced_tickers": ["0700.HK"],
                    }
                ],
                "",
            ),
        )
        detail, code = sink.fetch_filing_detail("f1")
        assert code == ""
        assert detail is not None
        assert detail["document_tables"] == [{"tableIndex": 0}]
        assert detail["referenced_tickers"] == ["0700.HK"]

    def test_not_found_is_none(self, monkeypatch):
        from hkex_scraper.sinks import clickhouse as ch

        sink = ch.ClickHouseSink()
        monkeypatch.setattr(sink, "_query", lambda sql, params=None: ([], ""))
        assert sink.fetch_filing_detail("nope") == (None, "")


class TestNeo4jDetail:
    def test_maps_camel_case_and_parses_tables(self, monkeypatch):
        from hkex_scraper.sinks import neo4j as neo

        sink = neo.Neo4jSink()
        record = {
            "filing_id": "f1",
            "company_ticker": "0451.HK",
            "document_tables": '[{"tableIndex": 0}]',
        }
        monkeypatch.setattr(sink, "_run", lambda query, params=None: ([record], None, ""))
        detail, code = sink.fetch_filing_detail("f1")
        assert code == ""
        assert detail is not None
        assert detail["company_ticker"] == "0451.HK"
        assert detail["document_tables"] == [{"tableIndex": 0}]

    def test_not_found_is_none(self, monkeypatch):
        from hkex_scraper.sinks import neo4j as neo

        sink = neo.Neo4jSink()
        monkeypatch.setattr(sink, "_run", lambda query, params=None: ([], None, ""))
        assert sink.fetch_filing_detail("nope") == (None, "")


class TestSurrealDetail:
    def test_maps_camel_case_to_snake_case(self, monkeypatch):
        from hkex_scraper.sinks import surrealdb as surreal

        row = {
            "filingId": "f1",
            "companyTicker": "0451.HK",
            "documentTables": [{"tableIndex": 0}],
            "referencedTickers": ["0700.HK"],
        }
        monkeypatch.setattr(surreal.db, "surreal_query", lambda sql, timeout=0: [{"result": [row]}])
        detail, code = surreal.SurrealDBSink().fetch_filing_detail("f1")
        assert code == ""
        assert detail is not None
        assert detail["filing_id"] == "f1"
        assert detail["company_ticker"] == "0451.HK"
        assert detail["document_tables"] == [{"tableIndex": 0}]

    def test_not_found_is_none(self, monkeypatch):
        from hkex_scraper.sinks import surrealdb as surreal

        monkeypatch.setattr(surreal.db, "surreal_query", lambda sql, timeout=0: [{"result": []}])
        assert surreal.SurrealDBSink().fetch_filing_detail("nope") == (None, "")


# ---------------------------------------------------------------------------
# Composable search conformance (relational engines)
# ---------------------------------------------------------------------------
def _rich_filing(filing_id, ticker, title, filing_type, when, refs=()):
    return {
        "filing_id": filing_id,
        "company_ticker": ticker,
        "stock_code": ticker.split(".")[0],
        "stock_name": f"Name {ticker}",
        "exchange": "HK",
        "filing_type": filing_type,
        "filing_subtype": filing_type,
        "filing_category": "LISTED_COMPANY",
        "title": title,
        "filing_date": when,
        "document_url": "https://example.invalid/%s.pdf" % filing_id,
        "referenced_tickers": list(refs),
        "source": "HKEx",
        "updated_at": when,
    }


def _seed_search(sink):
    sink.upsert_filings(
        [
            _rich_filing(
                "a",
                "0700.HK",
                "Annual Report 2024",
                "Annual Report",
                datetime(2024, 7, 1, tzinfo=timezone.utc),
                ["0005.HK"],
            ),
            _rich_filing(
                "b",
                "0005.HK",
                "Interim Results 2024",
                "Interim Results",
                datetime(2024, 3, 1, tzinfo=timezone.utc),
            ),
            _rich_filing(
                "c",
                "0005.HK",
                "Dividend Notice",
                "Dividend",
                datetime(2024, 9, 1, tzinfo=timezone.utc),
                ["0700.HK"],
            ),
        ]
    )


class TestSearchConformance:
    def test_ticker_and_type_filters(self, sink):
        from hkex_scraper.sinks.base import FilingQuery

        _seed_search(sink)
        rows, code = sink.search_filings(FilingQuery(tickers=("0005.HK",)), 0, 10)
        assert code == "" and {r["filing_id"] for r in rows} == {"b", "c"}

        rows, code = sink.search_filings(FilingQuery(filing_types=("Dividend",)), 0, 10)
        assert code == "" and [r["filing_id"] for r in rows] == ["c"]

        rows, code = sink.search_filings(FilingQuery(filing_categories=("LISTED_COMPANY",)), 0, 10)
        assert code == "" and len(rows) == 3

    def test_date_range_is_inclusive(self, sink):
        from hkex_scraper.sinks.base import FilingQuery

        _seed_search(sink)
        rows, code = sink.search_filings(
            FilingQuery(date_from="2024-06-01", date_to="2024-12-31"), 0, 10
        )
        assert code == "" and {r["filing_id"] for r in rows} == {"a", "c"}

        rows, code = sink.search_filings(FilingQuery(date_from="2024-09-01"), 0, 10)
        assert code == "" and [r["filing_id"] for r in rows] == ["c"]

    def test_referenced_ticker_and_order(self, sink):
        from hkex_scraper.sinks.base import FilingQuery

        _seed_search(sink)
        rows, code = sink.search_filings(FilingQuery(referenced_ticker="0700.HK"), 0, 10)
        assert code == "" and {r["filing_id"] for r in rows} == {"c"}

        rows, code = sink.search_filings(FilingQuery(), 0, 10)
        assert code == "" and [r["filing_id"] for r in rows] == ["c", "a", "b"]

        rows, code = sink.search_filings(FilingQuery(order_by="filing_date_asc"), 0, 10)
        assert code == "" and [r["filing_id"] for r in rows] == ["b", "a", "c"]

    def test_document_status_including_unprocessed(self, sink):
        from hkex_scraper.sinks.base import FilingQuery

        _seed_search(sink)
        sink.upsert_document("a", _doc("the quick brown fox"))

        rows, code = sink.search_filings(FilingQuery(document_status=("unprocessed",)), 0, 10)
        assert code == "" and {r["filing_id"] for r in rows} == {"b", "c"}

        rows, code = sink.search_filings(FilingQuery(document_status=("processed",)), 0, 10)
        assert code == "" and [r["filing_id"] for r in rows] == ["a"]

    def test_search_documents_returns_snippet(self, sink):
        from hkex_scraper.sinks.base import FilingQuery

        _seed_search(sink)
        sink.upsert_document("a", _doc("the quick brown fox jumps over"))

        rows, code = sink.search_documents(FilingQuery(text_query="brown"), 0, 10)
        assert code == "" and len(rows) == 1 and rows[0]["filing_id"] == "a"
        assert "brown" in rows[0]["snippet"]

        rows, code = sink.search_documents(FilingQuery(text_query="absent"), 0, 10)
        assert code == "" and rows == []

    def test_aggregate_by_ticker(self, sink):
        from hkex_scraper.sinks.base import FilingQuery

        _seed_search(sink)
        buckets, code = sink.aggregate_filings("company_ticker", FilingQuery())
        assert code == ""
        assert {b["key"]: b["count"] for b in buckets} == {"0005.HK": 2, "0700.HK": 1}

        buckets, code = sink.aggregate_filings("filing_type", FilingQuery(tickers=("0005.HK",)))
        assert code == "" and {b["key"] for b in buckets} == {"Interim Results", "Dividend"}

    def test_aggregate_rejects_unknown_group(self, sink):
        from hkex_scraper.sinks.base import FilingQuery

        buckets, code = sink.aggregate_filings("bogus", FilingQuery())
        assert buckets == [] and code.endswith("PAYLOAD_ERROR")

    def test_list_companies(self, sink):
        _seed_search(sink)
        rows, code = sink.list_companies(10, 0)
        assert code == ""
        assert rows[0] == {
            "company_ticker": "0005.HK",
            "stock_name": "Name 0005.HK",
            "filing_count": 2,
        }
