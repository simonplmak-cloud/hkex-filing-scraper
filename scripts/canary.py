#!/usr/bin/env python3
"""HKEx API canary — the early-warning control for risk R1.

Runs the real code path (JSF handshake → form POST → JSON pagination) against a small recent
window and asserts the response still looks like the documented shape. Exits non-zero when it
does not, so the scheduled workflow opens an issue instead of the scraper silently returning
nothing weeks later.

Live network by design: this is not part of the offline test suite. The logic it applies to a
response is unit-tested in ``tests/test_canary.py``.

Usage::

    python scripts/canary.py --days 7 --max 5 --json
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

from hkex_scraper import api, http

REQUIRED_FIELDS = ("date", "stockCode", "title", "link")


def check_records(records: List[dict], total: Optional[int]) -> Dict[str, Any]:
    """Validate a fetched page against the shape the scraper depends on."""
    missing = [
        index
        for index, record in enumerate(records)
        if not all(str(record.get(field) or "").strip() for field in REQUIRED_FIELDS)
    ]
    relative_links = [
        r.get("link", "") for r in records if not str(r.get("link", "")).startswith("http")
    ]

    checks = {
        "records_returned": len(records),
        "total_count_reported": total is not None,
        "total_count_value": total,
        "all_required_fields_present": not missing,
        "all_links_absolute": not relative_links,
    }
    problems = []
    if not records:
        problems.append("no records returned for the window")
    if missing:
        problems.append(f"{len(missing)} record(s) missing a required field: {missing[:5]}")
    if relative_links:
        problems.append(f"{len(relative_links)} relative link(s): {relative_links[:3]}")
    if not records and total in (0, None):
        problems.append("TOTAL_COUNT absent or zero, which the API normally reports")

    checks["ok"] = not problems
    checks["problems"] = problems
    return checks


def run(
    *,
    days: int = 7,
    max_records: int = 5,
    fetch: Callable[..., Tuple[List[dict], Optional[int]]] = api.fetch_chunk_via_api,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Fetch a small window and return a JSON-serialisable canary report."""
    end = now or datetime.now()
    start = end - timedelta(days=days)
    report: Dict[str, Any] = {
        "window": {
            "from": start.strftime("%Y-%m-%d"),
            "to": end.strftime("%Y-%m-%d"),
        },
        "ok": False,
    }
    try:
        records, total = fetch(
            http.get_session(),
            start.strftime("%Y%m%d"),
            end.strftime("%Y%m%d"),
            max_records=max_records,
        )
    except Exception as exc:  # noqa: BLE001 - the canary reports, it does not crash
        report["stage"] = "fetch"
        report["error"] = f"{type(exc).__name__}: {exc}"
        report["problems"] = ["the API request itself failed"]
        return report

    checks = check_records(records, total)
    report.update(checks)
    report["stage"] = "validate"
    sample = records[0] if records else None
    if sample is not None:
        # Titles and codes are public metadata; keep the sample small and non-identifying.
        report["sample"] = {
            "date": sample.get("date"),
            "stockCode": sample.get("stockCode"),
            "titleLength": len(str(sample.get("title") or "")),
        }
    return report


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=7, help="look-back window in days")
    parser.add_argument("--max", dest="max_records", type=int, default=5, help="records to fetch")
    parser.add_argument("--json", action="store_true", help="emit the report as JSON")
    args = parser.parse_args(argv)

    report = run(days=args.days, max_records=args.max_records)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        status = "OK" if report["ok"] else "FAILED"
        print(
            f"canary {status}: {report.get('records_returned', 0)} record(s) in {report['window']}"
        )
        for problem in report.get("problems", []):
            print(f"  - {problem}")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
