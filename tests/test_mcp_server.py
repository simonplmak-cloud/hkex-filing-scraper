"""Unit tests for the read-only MCP server tool surface.

Skipped when the optional ``mcp`` extra is not installed. The tool bodies are plain
functions, so they are exercised against a fake sink with no database and no network.
A dedicated test asserts the tools never write to stdout, because stdout carries the
MCP stdio protocol.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Tuple

import pytest

pytest.importorskip("mcp")

from mcp.server.fastmcp.exceptions import ToolError  # noqa: E402

from hkex_scraper import mcp_server  # noqa: E402
from hkex_scraper.sinks.base import FilingQuery, SinkCapabilities  # noqa: E402


class FakeSink:
    """A minimal in-memory read sink; any write call is a hard failure."""

    id = "sqlite"
    capabilities = SinkCapabilities(model="relational", reads=True, edges=True, snippets=True)

    _TITLES = [
        {
            "filing_id": "f1",
            "title": "Annual Report 2024",
            "stock_code": "00700",
            "company_ticker": "0700.HK",
        },
        {
            "filing_id": "f2",
            "title": "Interim Results 2024",
            "stock_code": "00001",
            "company_ticker": "0001.HK",
        },
    ]

    def available(self) -> bool:
        return True

    def unavailable_reason(self) -> str:
        return ""

    def count_filings(self) -> Tuple[int, str]:
        return 2, ""

    def distinct_company_tickers(self) -> Tuple[List[str], str]:
        return ["0700.HK", "0001.HK"], ""

    def fetch_titles(
        self, ticker_set: Optional[List[str]], offset: int, page_size: int, title_query: str = ""
    ) -> Tuple[List[Dict[str, Any]], str]:
        rows = list(self._TITLES)
        if ticker_set:
            rows = [r for r in rows if r["company_ticker"] in ticker_set]
        if title_query:
            rows = [r for r in rows if title_query.lower() in r["title"].lower()]
        return rows[offset : offset + page_size], ""

    def count_pending_filings(self) -> Tuple[int, str]:
        return 1, ""

    def fetch_pending_filings(self, limit: int) -> Tuple[List[Dict[str, Any]], str]:
        return [{"filing_id": "f2", "document_url": "https://example.invalid/x.pdf"}][:limit], ""

    def fetch_filing_detail(self, filing_id: str) -> Tuple[Optional[Dict[str, Any]], str]:
        if filing_id == "missing":
            return None, ""
        if filing_id == "boom":
            return None, "SQLITE_WRITE_ERROR: password=hunter2"
        return (
            {
                "filing_id": filing_id,
                "company_ticker": "0700.HK",
                "document_text": "x" * 100,
                "document_text_len": 100,
                "document_tables": [{"tableIndex": 0}],
                "document_status": "processed",
            },
            "",
        )

    def fetch_coverage(self) -> Tuple[List[Dict[str, Any]], str]:
        return [{"chunk_from": "2024-01-01", "api_count": 1, "ingested_count": 1}], ""

    def search_filings(
        self, query: FilingQuery, offset: int, limit: int
    ) -> Tuple[List[Dict[str, Any]], str]:
        rows = list(self._TITLES)
        if query.tickers:
            rows = [r for r in rows if r["company_ticker"] in query.tickers]
        if query.title_query:
            rows = [r for r in rows if query.title_query.lower() in r["title"].lower()]
        if query.document_status and "unprocessed" not in query.document_status:
            rows = []
        return rows[offset : offset + limit], ""

    def search_documents(
        self, query: FilingQuery, offset: int, limit: int
    ) -> Tuple[List[Dict[str, Any]], str]:
        rows, error_code = self.search_filings(query, offset, limit)
        for row in rows:
            row["snippet"] = "..." + (query.text_query or "") + "..."
        return rows, error_code

    def aggregate_filings(
        self, group_by: str, query: FilingQuery
    ) -> Tuple[List[Dict[str, Any]], str]:
        from collections import Counter

        rows, _ = self.search_filings(query, 0, 1000)
        counts = Counter(str(row.get(group_by)) for row in rows)
        buckets = [
            {"key": key, "count": count}
            for key, count in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
        ]
        return buckets, ""

    def list_companies(self, limit: int, offset: int) -> Tuple[List[Dict[str, Any]], str]:
        from collections import Counter

        rows, _ = self.search_filings(FilingQuery(), 0, 1000)
        counts = Counter(row["company_ticker"] for row in rows)
        companies = [
            {"company_ticker": ticker, "stock_name": None, "filing_count": count}
            for ticker, count in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
        ]
        return companies[offset : offset + limit], ""

    def read_filing_digests(self) -> Tuple[List[Dict[str, Any]], str]:
        return [{"filing_id": "f1", "document_sha256": "a" * 64}], ""

    # -- writes must never be reached -------------------------------------
    def _no_write(self, *args: Any, **kwargs: Any) -> None:
        raise AssertionError("a read-only tool attempted a write")

    upsert_filings = _no_write
    upsert_document = _no_write
    mark_status = _no_write
    insert_coverage = _no_write
    upsert_edges = _no_write
    ensure_schema = _no_write


@pytest.fixture()
def fake_read(monkeypatch):
    fake = FakeSink()
    monkeypatch.setattr(mcp_server.sinks, "read_sink", lambda: fake)
    monkeypatch.setattr(mcp_server.sinks, "enabled_sinks", lambda: [fake])
    return fake


@pytest.fixture()
def fake_registry(monkeypatch):
    monkeypatch.setattr(mcp_server.sinks, "known_ids", lambda: ["sqlite"])

    class _Spec:
        label = "SQLite"
        license = "Public domain"
        source_available = True
        extra = None

    monkeypatch.setattr(mcp_server.sinks, "spec", lambda sink_id: _Spec())


def test_tools_never_write_to_stdout(fake_read, capsys):
    mcp_server.get_server_info()
    mcp_server.list_sinks()
    mcp_server.get_config()
    mcp_server.count_filings()
    mcp_server.search_filings(ticker="0700.HK")
    mcp_server.get_filing("f1")
    captured = capsys.readouterr()
    assert captured.out == ""


def test_get_filing_windows_the_text_and_hides_tables(fake_read):
    detail = mcp_server.get_filing("f1", max_text_chars=10)
    assert detail["document_text"] == "x" * 10
    assert detail["text_offset"] == 0
    assert detail["text_limit"] == 10
    assert detail["text_total_chars"] == 100
    assert detail["text_truncated"] is True
    assert detail["next_text_offset"] == 10
    assert detail["document_tables"] is None


def test_get_filing_can_resume_and_include_tables(fake_read):
    detail = mcp_server.get_filing("f1", text_offset=90, max_text_chars=50, include_tables=True)
    assert detail["document_text"] == "x" * 10
    assert detail["text_truncated"] is False
    assert detail["next_text_offset"] is None
    assert detail["document_tables"] == [{"tableIndex": 0}]


def test_get_filing_omits_text_when_asked(fake_read):
    detail = mcp_server.get_filing("f1", include_text=False)
    assert detail["document_text"] is None
    assert detail["text_truncated"] is False


def test_get_filing_not_found_is_an_actionable_error(fake_read):
    with pytest.raises(ToolError) as excinfo:
        mcp_server.get_filing("missing")
    assert "not found" in str(excinfo.value)
    assert "search_filings" in str(excinfo.value)


def test_get_filing_sink_error_is_redacted(fake_read):
    with pytest.raises(ToolError) as excinfo:
        mcp_server.get_filing("boom")
    assert "password=***" in str(excinfo.value)
    assert "hunter2" not in str(excinfo.value)


def test_search_filings_filters_by_title(fake_read):
    result = mcp_server.search_filings(title_query="annual")
    assert [row["filing_id"] for row in result["items"]] == ["f1"]
    assert result["returned_count"] == 1


def test_ticker_paging_reports_completeness(fake_read):
    result = mcp_server.list_tickers(limit=1)
    assert result["returned_count"] == 1
    assert result["total_count"] == 2
    assert result["has_more"] is True
    assert result["next_offset"] == 1


def test_count_filings_shape(fake_read):
    result = mcp_server.count_filings()
    assert result["counts"] == {"sqlite": 2}
    assert result["read_sink"] == "sqlite"


def test_list_sinks_reports_capabilities(fake_read, fake_registry):
    result = mcp_server.list_sinks()
    assert result["read_sink"] == "sqlite"
    (row,) = result["sinks"]
    assert row["id"] == "sqlite"
    assert row["configured"] is True
    assert row["available"] is True
    assert row["is_read_sink"] is True
    assert row["capabilities"]["reads"] is True


def test_verify_requires_two_sinks(fake_read):
    with pytest.raises(ToolError):
        mcp_server.verify_sinks()


def test_verify_reports_disagreement(monkeypatch):
    good = FakeSink()

    class Other(FakeSink):
        id = "duckdb"

        def read_filing_digests(self):
            return [{"filing_id": "f1", "document_sha256": "b" * 64}], ""

    monkeypatch.setattr(mcp_server.sinks, "enabled_sinks", lambda: [good, Other()])
    result = mcp_server.verify_sinks()
    assert result["ok"] is False
    assert result["problems"][0]["kind"] == "hash_mismatch"


def test_no_read_sink_is_an_actionable_error(monkeypatch):
    monkeypatch.setattr(mcp_server.sinks, "enabled_sinks", lambda: [])
    monkeypatch.setattr(mcp_server.sinks, "read_sink", lambda: None)
    with pytest.raises(ToolError) as excinfo:
        mcp_server.list_tickers()
    assert "reads" in str(excinfo.value)


def test_build_server_registers_every_tool_read_only():
    server = mcp_server.build_server()
    tools = asyncio.run(server.list_tools())
    names = {tool.name for tool in tools}
    assert names == {fn.__name__ for fn in mcp_server.TOOLS}
    for tool in tools:
        assert tool.annotations is not None
        assert tool.annotations.readOnlyHint is True
        assert tool.annotations.destructiveHint is False
        assert tool.annotations.openWorldHint is False


def test_search_documents_requires_query(fake_read):
    with pytest.raises(ToolError):
        mcp_server.search_documents(text_query="")


def test_search_documents_returns_snippets(fake_read):
    result = mcp_server.search_documents("annual")
    assert result["snippets_supported"] is True
    assert result["items"][0]["snippet"].startswith("...annual")


def test_search_filings_rejects_a_bad_date(fake_read):
    with pytest.raises(ToolError) as excinfo:
        mcp_server.search_filings(date_from="2024/01/01")
    assert "ISO date" in str(excinfo.value)


def test_get_statistics_groups_by_ticker(fake_read):
    result = mcp_server.get_statistics("company_ticker")
    assert result["group_by"] == "company_ticker"
    assert {bucket["key"] for bucket in result["buckets"]} == {"0700.HK", "0001.HK"}


def test_get_statistics_rejects_an_unknown_group(fake_read):
    with pytest.raises(ToolError):
        mcp_server.get_statistics("nope")


def test_list_companies_shape(fake_read):
    result = mcp_server.list_companies()
    assert result["items"][0]["company_ticker"] in {"0700.HK", "0001.HK"}
    assert "filing_count" in result["items"][0]


def test_get_filings_batch(fake_read):
    result = mcp_server.get_filings(["f1", "missing"])
    assert result["returned_count"] == 1
    assert result["not_found"] == ["missing"]


def test_get_coverage_totals(fake_read):
    result = mcp_server.get_coverage()
    assert result["totals"]["api_count"] == 1
    assert result["totals"]["coverage_percent"] == 0.0


def test_list_pending_filings_status_filter(fake_read):
    result = mcp_server.list_pending_filings(document_status="unprocessed")
    assert result["document_status"] == ["unprocessed"]
    assert result["returned_count"] == 2
