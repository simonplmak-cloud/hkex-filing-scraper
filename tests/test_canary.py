"""Unit tests for the canary's validation logic (no network).

The canary runs live in CI on a schedule; these cover the decision it makes about a response,
including every failure mode that should raise an issue.
"""

from __future__ import annotations

import importlib.util
import pathlib
from datetime import datetime

ROOT = pathlib.Path(__file__).resolve().parent.parent


def _canary():
    path = ROOT / "scripts" / "canary.py"
    spec = importlib.util.spec_from_file_location("canary", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


GOOD = {
    "date": "11/02/2026",
    "stockCode": "01461",
    "stockName": "ZHONGTAI FUTURES",
    "title": "Announcement",
    "link": "https://www1.hkexnews.hk/listedco/listconews/sehk/2026/0211/a.pdf",
}


class TestCheckRecords:
    def test_healthy_page_passes(self):
        checks = _canary().check_records([GOOD], total=2)
        assert checks["ok"] is True
        assert checks["problems"] == []
        assert checks["records_returned"] == 1

    def test_no_records_is_a_failure(self):
        checks = _canary().check_records([], total=0)
        assert checks["ok"] is False
        assert any("no records" in p for p in checks["problems"])

    def test_missing_required_field_is_a_failure(self):
        broken = dict(GOOD, title="")
        checks = _canary().check_records([broken], total=1)
        assert checks["ok"] is False
        assert checks["all_required_fields_present"] is False

    def test_relative_link_is_a_failure(self):
        broken = dict(GOOD, link="/listedco/x.pdf")
        checks = _canary().check_records([broken], total=1)
        assert checks["ok"] is False
        assert checks["all_links_absolute"] is False

    def test_missing_total_count_is_reported_but_not_fatal_with_records(self):
        checks = _canary().check_records([GOOD], total=None)
        assert checks["total_count_reported"] is False
        assert checks["ok"] is True, "records are the primary signal"


class TestRun:
    def test_successful_fetch_reports_ok(self):
        canary = _canary()
        report = canary.run(
            now=datetime(2026, 2, 11),
            fetch=lambda *a, **k: ([GOOD], 2),
        )
        assert report["ok"] is True
        assert report["stage"] == "validate"
        assert report["window"] == {"from": "2026-02-04", "to": "2026-02-11"}
        assert report["sample"]["stockCode"] == "01461"
        assert "title" not in report["sample"], "only a length is reported, not the title"

    def test_fetch_exception_is_reported_not_raised(self):
        canary = _canary()

        def boom(*args, **kwargs):
            raise OSError("connection reset")

        report = canary.run(fetch=boom)
        assert report["ok"] is False
        assert report["stage"] == "fetch"
        assert "OSError" in report["error"]
        assert report["problems"] == ["the API request itself failed"]

    def test_empty_result_is_reported_as_a_problem(self):
        canary = _canary()
        report = canary.run(fetch=lambda *a, **k: ([], None))
        assert report["ok"] is False
        assert any("no records" in p for p in report["problems"])


class TestMain:
    def test_exit_code_matches_the_report(self, monkeypatch, capsys):
        canary = _canary()
        monkeypatch.setattr(
            canary,
            "run",
            lambda **kwargs: {
                "ok": True,
                "records_returned": 3,
                "window": {"from": "a", "to": "b"},
                "problems": [],
            },
        )
        assert canary.main(["--json"]) == 0
        assert '"ok": true' in capsys.readouterr().out

    def test_non_zero_exit_on_failure(self, monkeypatch, capsys):
        canary = _canary()
        monkeypatch.setattr(
            canary,
            "run",
            lambda **kwargs: {
                "ok": False,
                "records_returned": 0,
                "window": {"from": "a", "to": "b"},
                "problems": ["no records"],
            },
        )
        assert canary.main([]) == 1
        assert "FAILED" in capsys.readouterr().out

    def test_days_are_forwarded(self, monkeypatch):
        canary = _canary()
        seen = {}

        def fake_run(**kwargs):
            seen.update(kwargs)
            return {"ok": True, "records_returned": 1, "window": {}, "problems": []}

        monkeypatch.setattr(canary, "run", fake_run)
        canary.main(["--days", "3", "--max", "2"])
        assert seen["days"] == 3 and seen["max_records"] == 2
