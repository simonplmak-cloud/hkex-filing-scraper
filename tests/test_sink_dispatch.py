"""Dispatch tests: pipeline and graph route records to every configured sink.

Uses in-memory fake sinks so no database is required.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from hkex_scraper import pipeline, sinks
from hkex_scraper.sinks.base import ERR_NONE, Sink, SinkCapabilities


class FakeSink(Sink):
    def __init__(
        self,
        sink_id: str,
        available: bool = True,
        fail: bool = False,
        caps: Optional[SinkCapabilities] = None,
    ) -> None:
        self.id = sink_id
        self.capabilities = caps or SinkCapabilities()
        self._available = available
        self.fail = fail
        self.calls: List[Tuple[str, Any]] = []
        self._filings: Dict[str, Dict[str, Any]] = {}

    def available(self) -> bool:
        return self._available

    def unavailable_reason(self) -> str:
        return f"{self.id} sink unavailable (test)"

    def upsert_filings(self, records: List[Dict[str, Any]]) -> Tuple[int, str]:
        self.calls.append(("filings", len(records)))
        for record in records:
            self._filings[record["filing_id"]] = record
        if self.fail:
            return 0, f"{self.id.upper()}_WRITE_ERROR"
        return len(records), ERR_NONE

    def upsert_document(self, filing_id: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
        self.calls.append(("document", filing_id))
        return (False, f"{self.id.upper()}_WRITE_ERROR") if self.fail else (True, ERR_NONE)

    def mark_status(self, filing_id: str, status: str, reason: str = "") -> Tuple[bool, str]:
        self.calls.append(("status", (filing_id, status)))
        return (False, f"{self.id.upper()}_WRITE_ERROR") if self.fail else (True, ERR_NONE)

    def insert_coverage(self, chunk: Dict[str, Any]) -> Tuple[bool, str]:
        self.calls.append(("coverage", chunk.get("run_id")))
        return (False, f"{self.id.upper()}_WRITE_ERROR") if self.fail else (True, ERR_NONE)

    def upsert_edges(self, edges: List[Dict[str, Any]], kind: str) -> Tuple[int, str]:
        self.calls.append((kind, len(edges)))
        if self.fail:
            return 0, f"{self.id.upper()}_WRITE_ERROR"
        return len(edges), ERR_NONE

    def count_filings(self) -> Tuple[int, str]:
        return len(self._filings), ERR_NONE

    def count_edges(self, kind: str) -> Tuple[int, str]:
        return 7, ERR_NONE


def _install(monkeypatch, fakes: List[FakeSink]) -> None:
    monkeypatch.setattr(sinks, "enabled_sinks", lambda: fakes)


def _sample_filing() -> dict:
    return {
        "stockCode": "0451",
        "date": "01/07/2024",
        "title": "Annual Report 2024",
        "stockName": "Example Holdings",
        "link": "https://www1.hkexnews.hk/example.pdf",
    }


class TestMetadataDispatch:
    def test_writes_to_every_sink(self, monkeypatch):
        a = FakeSink("postgres")
        b = FakeSink("sqlite")
        _install(monkeypatch, [a, b])
        pipeline.reset_sink_stats()

        saved = pipeline._save_filings_batch_metadata([_sample_filing()])
        assert saved == 1
        assert ("filings", 1) in a.calls and ("filings", 1) in b.calls
        assert pipeline.SINK_STATS["postgres"]["ok"] == 1
        assert pipeline.SINK_STATS["sqlite"]["ok"] == 1
        assert pipeline.sink_exit_code() == 0

    def test_failure_isolation_and_counters(self, monkeypatch):
        a = FakeSink("postgres")
        b = FakeSink("sqlite", fail=True)
        _install(monkeypatch, [a, b])
        pipeline.reset_sink_stats()

        saved = pipeline._save_filings_batch_metadata([_sample_filing()])
        assert saved == 1  # the healthy sink's write is retained
        assert pipeline.SINK_STATS["postgres"]["ok"] == 1
        assert pipeline.SINK_STATS["sqlite"]["failed"] == 1
        assert pipeline.sink_exit_code() == 1

    def test_unavailable_sink_warns_and_skips(self, monkeypatch, capsys):
        a = FakeSink("sqlite")
        b = FakeSink("mysql", available=False)
        _install(monkeypatch, [a, b])
        pipeline.reset_sink_stats()

        saved = pipeline._save_filings_batch_metadata([_sample_filing()])
        out = capsys.readouterr().out
        assert saved == 1
        assert "mysql sink unavailable" in out
        assert pipeline.sink_exit_code() == 0


class TestDocumentAndStatusDispatch:
    def test_document_reaches_every_sink(self, monkeypatch):
        a = FakeSink("postgres")
        b = FakeSink("sqlite")
        _install(monkeypatch, [a, b])
        pipeline.reset_sink_stats()

        ok, code = pipeline._save_document_to_filing(
            "fid1", b"body", 4, "https://x/doc.pdf", "text", [{"tableIndex": 0}]
        )
        assert ok and code == ""
        assert ("document", "fid1") in a.calls and ("document", "fid1") in b.calls

    def test_document_failure_on_one_sink(self, monkeypatch):
        a = FakeSink("postgres")
        b = FakeSink("sqlite", fail=True)
        _install(monkeypatch, [a, b])
        pipeline.reset_sink_stats()

        ok, code = pipeline._save_document_to_filing(
            "fid2", b"body", 4, "https://x/doc.pdf", "text", []
        )
        assert ok is False and code == "SQLITE_WRITE_ERROR"
        assert pipeline.SINK_STATS["sqlite"]["failed"] == 1

    def test_status_reaches_every_sink(self, monkeypatch):
        a = FakeSink("postgres")
        b = FakeSink("sqlite")
        _install(monkeypatch, [a, b])
        pipeline.reset_sink_stats()

        assert pipeline._mark_filing_status("fid3", "skipped", "no_document_url") is True
        assert ("status", ("fid3", "skipped")) in a.calls
        assert ("status", ("fid3", "skipped")) in b.calls

    def test_coverage_reaches_every_sink(self, monkeypatch):
        a = FakeSink("postgres")
        b = FakeSink("sqlite")
        _install(monkeypatch, [a, b])
        pipeline.reset_sink_stats()

        pipeline._persist_coverage({"run_id": "r1", "api_count": 1})
        assert ("coverage", "r1") in a.calls and ("coverage", "r1") in b.calls


class TestGraphDispatch:
    def test_edges_are_dispatched(self, monkeypatch):
        from hkex_scraper import graph

        monkeypatch.setattr(graph, "COMPANY_TABLE", "company")

        class Reader(FakeSink):
            def distinct_company_tickers(self):
                return ["0451.HK"], ERR_NONE

            def fetch_filing_ids_by_ticker(self, tickers):
                return [{"company_ticker": "0451.HK", "filing_id": "f1"}], ERR_NONE

        reader = Reader("postgres")
        edge_sink = FakeSink("sqlite")
        monkeypatch.setattr(sinks, "read_sink", lambda: reader)
        monkeypatch.setattr(sinks, "edge_sinks", lambda: [edge_sink])
        pipeline.reset_sink_stats()

        total = graph.link_filings_to_companies()
        assert ("has_filing", 1) in edge_sink.calls
        assert total == 7  # reader.count_edges

    def test_cross_references_are_dispatched(self, monkeypatch):
        from hkex_scraper import graph

        monkeypatch.setattr(graph, "COMPANY_TABLE", "company")

        class Reader(FakeSink):
            def fetch_titles(self, ticker_set, offset, page_size):
                return [
                    {
                        "filing_id": "f1",
                        "title": "Disclosure of interest in stock code: 0700",
                        "stock_code": "0451",
                        "company_ticker": "0451.HK",
                    }
                ], ERR_NONE

        edge_sink = FakeSink("sqlite")
        monkeypatch.setattr(sinks, "read_sink", lambda: Reader("postgres"))
        monkeypatch.setattr(sinks, "edge_sinks", lambda: [edge_sink])
        pipeline.reset_sink_stats()

        graph.cross_reference_filings()
        kinds = [call[0] for call in edge_sink.calls]
        assert "references_filing" in kinds


class TestParityReport:
    def test_parity_ok(self):
        from hkex_scraper.main import _format_parity_report

        report = _format_parity_report({"postgres": 10, "sqlite": 10})
        assert "Parity: OK" in report
        assert "WARNING" not in report

    def test_parity_spread_is_warned(self):
        from hkex_scraper.main import _format_parity_report

        report = _format_parity_report({"postgres": 10, "sqlite": 8, "mysql": 9})
        assert "WARNING" in report
        assert "Spread:            2" in report

    def test_parity_na_for_single_sink(self):
        from hkex_scraper.main import _format_parity_report

        report = _format_parity_report({"postgres": 3})
        assert "Parity: N/A" in report
