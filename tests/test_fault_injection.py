"""Fault-injection tests for sink failure isolation.

The contract is: every configured sink is attempted, one sink's failure never blocks another,
the failure is counted, and the run exits non-zero. These use fake sinks so no database is
needed — the point is the dispatch logic, not the drivers.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pytest

from hkex_scraper import pipeline, sinks
from hkex_scraper.sinks.base import Sink, SinkCapabilities


class FakeSink(Sink):
    """A sink whose behaviour is scripted per test."""

    def __init__(
        self, sink_id: str, *, available: bool = True, fail: bool = False, raises: bool = False
    ):
        super().__init__()
        self._id = sink_id
        self._available = available
        self._fail = fail
        self._raises = raises
        self.filings: List[Dict[str, Any]] = []
        self.documents: List[Tuple[str, Dict[str, Any]]] = []

    # -- identity / capability ------------------------------------------------
    @property
    def id(self) -> str:  # type: ignore[override]
        return self._id

    @property
    def label(self) -> str:  # type: ignore[override]
        return self._id

    @property
    def capabilities(self) -> SinkCapabilities:  # type: ignore[override]
        return SinkCapabilities(upsert=True, reads=True, edges=True, json=True, arrays=True)

    def available(self) -> bool:
        return self._available

    def unavailable_reason(self) -> str:
        return f"{self._id} sink requires configuration"

    def close(self) -> None:
        return None

    # -- writes ---------------------------------------------------------------
    def upsert_filings(self, records: List[Dict[str, Any]]) -> Tuple[int, str]:
        if self._raises:
            raise RuntimeError("boom")
        if self._fail:
            return 0, f"{self._id.upper()}_WRITE_ERROR"
        self.filings.extend(records)
        return len(records), ""

    def upsert_document(self, filing_id: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
        if self._raises:
            raise RuntimeError("boom")
        if self._fail:
            return False, f"{self._id.upper()}_WRITE_ERROR"
        self.documents.append((filing_id, payload))
        return True, ""

    def mark_status(self, filing_id: str, status: str, reason: str = "") -> Tuple[bool, str]:
        if self._raises:
            raise RuntimeError("boom")
        return (not self._fail), (f"{self._id.upper()}_WRITE_ERROR" if self._fail else "")


@pytest.fixture
def fake_sinks(monkeypatch):
    """Install a set of scripted sinks as the configured sinks."""
    registry: Dict[str, FakeSink] = {}

    def install(*sink_list: FakeSink):
        for sink in sink_list:
            registry[sink.id] = sink
        monkeypatch.setattr(sinks, "enabled_sinks", lambda: list(sink_list))
        monkeypatch.setattr(pipeline, "_configured_sinks", lambda: list(sink_list))
        pipeline.reset_sink_stats()
        return sink_list

    return install


PAYLOAD = b"%PDF-1.4 sample"


class TestFailureIsolation:
    def test_a_failing_sink_does_not_block_the_others(self, fake_sinks):
        good = FakeSink("postgres")
        bad = FakeSink("sqlite", fail=True)
        fake_sinks(good, bad)

        ok, error = pipeline._save_document_to_filing("fid1", PAYLOAD, len(PAYLOAD), "x.pdf")

        assert good.documents, "the healthy sink must still receive the document"
        assert not bad.documents
        assert ok is False
        assert error == "SQLITE_WRITE_ERROR"

    def test_a_raising_sink_is_caught_and_counted(self, fake_sinks):
        good = FakeSink("postgres")
        explosive = FakeSink("mysql", raises=True)
        fake_sinks(good, explosive)

        ok, error = pipeline._save_document_to_filing("fid2", PAYLOAD, len(PAYLOAD), "x.pdf")

        assert good.documents
        assert ok is False
        assert error == "mysql_exception"
        assert pipeline.SINK_STATS["mysql"]["failed"] == 1

    def test_an_unavailable_sink_is_skipped_not_failed(self, fake_sinks):
        good = FakeSink("postgres")
        offline = FakeSink("mongodb", available=False)
        fake_sinks(good, offline)

        ok, error = pipeline._save_document_to_filing("fid3", PAYLOAD, len(PAYLOAD), "x.pdf")

        assert ok is True and error == ""
        assert pipeline.SINK_STATS["mongodb"]["failed"] == 0
        assert not offline.documents

    def test_all_healthy_sinks_report_success(self, fake_sinks):
        a = FakeSink("postgres")
        b = FakeSink("sqlite")
        fake_sinks(a, b)

        ok, error = pipeline._save_document_to_filing("fid4", PAYLOAD, len(PAYLOAD), "x.pdf")

        assert (ok, error) == (True, "")
        assert a.documents and b.documents


class TestExitCodes:
    def test_exit_code_is_zero_when_every_sink_succeeds(self, fake_sinks):
        good = FakeSink("postgres")
        fake_sinks(good)
        pipeline._save_document_to_filing("fid5", PAYLOAD, len(PAYLOAD), "x.pdf")
        assert pipeline.sink_exit_code() == 0

    def test_exit_code_is_non_zero_when_any_sink_failed(self, fake_sinks):
        good = FakeSink("postgres")
        bad = FakeSink("sqlite", fail=True)
        fake_sinks(good, bad)
        pipeline._save_document_to_filing("fid6", PAYLOAD, len(PAYLOAD), "x.pdf")
        assert pipeline.sink_exit_code() == 1

    def test_status_updates_follow_the_same_isolation(self, fake_sinks):
        good = FakeSink("postgres")
        bad = FakeSink("sqlite", fail=True)
        fake_sinks(good, bad)

        ok = pipeline._mark_filing_status("fid7", "skipped", "too_large")

        assert ok is False
        assert pipeline.SINK_STATS["postgres"]["failed"] == 0
        assert pipeline.SINK_STATS["sqlite"]["failed"] >= 1


class TestStatsBookkeeping:
    def test_counters_start_at_zero_for_every_sink(self, fake_sinks):
        fake_sinks(FakeSink("postgres"), FakeSink("sqlite"))
        assert pipeline.SINK_STATS == {
            "postgres": {"ok": 0, "failed": 0},
            "sqlite": {"ok": 0, "failed": 0},
        }

    def test_record_sink_accumulates_and_reset_clears(self):
        pipeline.reset_sink_stats()
        pipeline.record_sink("postgres", True, 3)
        pipeline.record_sink("postgres", False)
        assert pipeline.SINK_STATS["postgres"] == {"ok": 3, "failed": 1}
        pipeline.reset_sink_stats()
        assert pipeline.SINK_STATS.get("postgres", {"ok": 0, "failed": 0}) == {"ok": 0, "failed": 0}
