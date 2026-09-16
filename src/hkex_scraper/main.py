"""CLI entry point and argument parsing."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from typing import Dict

from . import __version__, config, sinks
from .config import LOG_DIR, MAX_DOWNLOAD_WORKERS
from .extractor import check_dependencies
from .graph import cross_reference_filings, link_filings_to_companies
from .pipeline import (
    log_sink_summary,
    reset_sink_stats,
    run_phase1,
    run_phase2,
    sink_exit_code,
)
from .utils import close_log_file, log, set_log_file


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _validate_env() -> None:
    """Ensure DATABASE_TARGET is explicit, known, and every sink is usable."""
    ids = config.sink_ids()
    if not ids:
        log(
            "ERROR: DATABASE_TARGET is not set. "
            f"Set it to one or more sink ids (e.g. DATABASE_TARGET={sinks.DEFAULT_SINK}). "
            f"Valid sinks: {', '.join(sinks.known_ids())}"
        )
        sys.exit(1)

    try:
        for sink_id in ids:
            sinks.spec(sink_id)
    except sinks.UnknownSinkError as exc:
        log(f"ERROR: {exc}")
        sys.exit(1)

    unusable = [sink for sink in sinks.enabled_sinks() if not sink.available()]
    if unusable:
        for sink in unusable:
            log(f"ERROR: {sink.unavailable_reason()}")
        log("ERROR: refusing to start with an unusable configured sink.")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Reporting helpers
# ---------------------------------------------------------------------------


def _format_parity_report(counts: Dict[str, int]) -> str:
    """Format an N-way per-sink filing-count parity report."""
    lines = ["PARITY REPORT", "=" * 60]
    for sink_id, count in counts.items():
        lines.append(f"{sinks.spec(sink_id).label} filings: {count}")
    if len(counts) < 2:
        lines.append("Parity: N/A (configure two or more sinks to compare)")
        return "\n".join(lines)
    spread = max(counts.values()) - min(counts.values())
    lines.append(f"Spread:            {spread}")
    if spread == 0:
        lines.append("Parity: OK (0 spread)")
    else:
        lines.append(f"WARNING: sinks differ by {spread} record(s)")
    return "\n".join(lines)


def _print_coverage(records: list) -> None:
    """Print a coverage table from canonical coverage rows."""
    if not records:
        log("No coverage data found. Run the scraper first to generate coverage stats.")
        return
    print(
        f"{'From':<12} {'To':<12} {'API':>6} {'Raw':>6} {'Unique':>6} "
        f"{'%':>6} {'Run ID':<18} {'Timestamp'}"
    )
    print("-" * 90)
    for r in records:
        cf, ct = r.get("chunk_from"), r.get("chunk_to")
        api = r.get("api_count", 0) or 0
        ingested = r.get("ingested_count", 0) or 0
        unique = r.get("unique_count", 0) or 0
        rid = r.get("run_id", "-") or "-"
        ts = r.get("timestamp", "-") or "-"
        cf_s = str(cf)[:10] if cf else "-"
        ct_s = str(ct)[:10] if ct else "-"
        pct = f"{(unique / api * 100):.1f}" if api > 0 else "N/A"
        print(
            f"{cf_s:<12} {ct_s:<12} {api:>6} {ingested:>6} {unique:>6} {pct:>5}% {str(rid):<18} {ts}"
        )
    print("-" * 90)
    print(f"{len(records)} coverage records")


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hkex-scraper",
        description=(
            "Scrape and ingest HKEx (Hong Kong Stock Exchange) regulatory filings "
            "into one or more database sinks. Full pipeline runs by default: "
            "metadata scrape, document download, text extraction, and graph linking."
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "--full-history",
        action="store_true",
        help="Scrape all filings from April 1999 to today",
    )
    parser.add_argument(
        "--from-date",
        type=str,
        default="",
        metavar="DD/MM/YYYY",
        help="Start date for scraping (default: ~2 months ago)",
    )
    parser.add_argument(
        "--to-date",
        type=str,
        default="",
        metavar="DD/MM/YYYY",
        help="End date for scraping (default: today)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        metavar="N",
        help="Limit the number of filings to process (0 = unlimited)",
    )
    parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Phase 1 only: scrape metadata without downloading documents",
    )
    parser.add_argument(
        "--backfill-docs",
        action="store_true",
        help="Phase 2 only: download documents for existing filings in the database",
    )
    parser.add_argument(
        "--link-only",
        action="store_true",
        help="Only create/refresh graph edges (no scraping or downloading)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Test mode: fetch data but do not write to the database",
    )
    parser.add_argument(
        "--coverage-report",
        action="store_true",
        help="Print historical coverage data from the read source and exit",
    )
    parser.add_argument(
        "--database-target",
        type=str,
        default="",
        metavar="SINKS",
        help=(
            "Override DATABASE_TARGET for this run as a comma-separated list "
            f"(valid: {', '.join(sinks.known_ids())}). Order sets read precedence."
        ),
    )
    parser.add_argument(
        "--parity-report",
        action="store_true",
        help="Print per-sink filing counts and the spread, then exit",
    )
    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _init_schemas() -> None:
    for sink in sinks.enabled_sinks():
        ok, err_code = sink.ensure_schema()
        if ok:
            log(f"  {sink.id} schema initialized successfully")
        else:
            log(f"ERROR: {sink.id} schema initialization failed: {err_code[:300]}")
            sys.exit(1)


def _parity_report() -> None:
    log("PARITY REPORT")
    if len(sinks.enabled_sinks()) < 2:
        log("ERROR: --parity-report requires two or more configured sinks.")
        sys.exit(1)
    counts: Dict[str, int] = {}
    for sink in sinks.enabled_sinks():
        count, err_code = sink.count_filings()
        if err_code:
            log(f"ERROR: could not count filings on {sink.id}: {err_code[:200]}")
            sys.exit(1)
        counts[sink.id] = count
    print(_format_parity_report(counts))
    sys.exit(0)


def _coverage_report() -> None:
    log("COVERAGE REPORT")
    log("=" * 60)
    reader = sinks.read_sink()
    if reader is None:
        log("ERROR: no configured sink supports reads for the coverage report.")
        sys.exit(1)
    records, err_code = reader.fetch_coverage()
    if err_code:
        log(f"ERROR: could not read coverage from {reader.id}: {err_code[:200]}")
        sys.exit(1)
    _print_coverage(records)
    sys.exit(0)


def main() -> None:
    """Entry point for the ``hkex-scraper`` CLI."""
    parser = _build_parser()
    args = parser.parse_args()

    if args.database_target:
        config.apply_database_target(args.database_target)

    check_dependencies()
    _validate_env()

    LOG_DIR.mkdir(exist_ok=True)
    log_path = LOG_DIR / f"hkex_filings_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.log"
    set_log_file(open(log_path, "w", encoding="utf-8"))

    try:
        reset_sink_stats()
        log(f"Database target: {config.DATABASE_TARGET} (sinks: {', '.join(config.sink_ids())})")

        _init_schemas()

        if args.parity_report:
            _parity_report()

        if args.coverage_report:
            _coverage_report()

        if args.backfill_docs:
            # Phase 2 only
            run_phase2(
                batch_size=50,
                max_workers=MAX_DOWNLOAD_WORKERS,
                limit=args.limit,
            )
        elif args.link_only:
            log("=" * 60)
            log("LINKING FILINGS TO COMPANIES")
            log("=" * 60)
            count = link_filings_to_companies()
            log(f"Created {count} edges")
            log("")
            xref_count = cross_reference_filings()
            log(f"Created {xref_count} cross-reference edges")
        else:
            # Phase 1: scrape metadata
            total, ingested_tickers = run_phase1(
                max_filings=args.limit,
                dry_run=args.dry_run,
                date_from=args.from_date,
                date_to=args.to_date,
                full_history=args.full_history,
            )
            if not args.dry_run and total > 0:
                log("")
                count = link_filings_to_companies(ticker_set=ingested_tickers)
                log(f"Created {count} edges")
                log("")
                xref_count = cross_reference_filings(ticker_set=ingested_tickers)
                log(f"Created {xref_count} cross-reference edges")
                log("")

                # Phase 2: download documents (default unless --metadata-only)
                if not args.metadata_only:
                    log("Proceeding to Phase 2: Document download & text extraction...")
                    log("")
                    run_phase2(
                        batch_size=50,
                        max_workers=MAX_DOWNLOAD_WORKERS,
                        limit=args.limit,
                    )
                else:
                    log("Skipping document downloads (--metadata-only)")
                    log("Run again with --backfill-docs to download documents later.")
                    log("")

        log_sink_summary()
        log("=" * 60)
        log("COMPLETE")
        log("=" * 60)
        log(f"Log: {log_path}")
        exit_code = sink_exit_code()
        if exit_code:
            log("ERROR: one or more sink writes failed (see sink summary above)")
        sys.exit(exit_code)
    finally:
        close_log_file()
        sinks.close_all()


if __name__ == "__main__":
    main()
