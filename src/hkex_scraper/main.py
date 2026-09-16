"""CLI entry point and argument parsing."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime

from . import __version__, config, db_postgres
from .config import LOG_DIR, MAX_DOWNLOAD_WORKERS, SURREAL_ENDPOINT, SURREAL_PASS
from .db import initialize_schema, surreal_query
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


def _required_sink_error() -> str:
    """Return an actionable message when a required sink is unusable, else ``""``."""
    if not config.postgres_required():
        return ""
    if not db_postgres.driver_installed():
        return (
            "PostgreSQL is the only configured sink but the psycopg driver is not "
            'installed. Install with: pip install ".[postgres]"'
        )
    if not config.postgres_conninfo():
        return (
            "PostgreSQL is the only configured sink but no connection details are set "
            "(set POSTGRES_DSN or POSTGRES_DATABASE/POSTGRES_USER)."
        )
    return ""


def _validate_env() -> None:
    """Ensure the required variables for the selected sink(s) are set and usable."""
    missing: list[str] = []
    if config.surrealdb_enabled():
        if not SURREAL_ENDPOINT:
            missing.append("SURREAL_ENDPOINT")
        if not SURREAL_PASS:
            missing.append("SURREAL_PASSWORD")
    sink_error = _required_sink_error()
    if missing or sink_error:
        if missing:
            log(f"ERROR: Missing required env vars: {', '.join(missing)}")
        if sink_error:
            log(f"ERROR: {sink_error}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Reporting helpers
# ---------------------------------------------------------------------------


def _format_parity_report(surreal_count: int, pg_count: int, surrealdb_enabled: bool = True) -> str:
    """Format a per-sink filing-count parity report."""
    lines = [
        "PARITY REPORT",
        "=" * 60,
        f"SurrealDB filings: {surreal_count}",
        f"PostgreSQL filings: {pg_count}",
    ]
    if not surrealdb_enabled:
        lines.append("Parity: N/A (SurrealDB sink disabled; use DATABASE_TARGET=both to compare)")
        return "\n".join(lines)
    delta = abs(surreal_count - pg_count)
    lines.append(f"Difference:        {delta}")
    if delta == 0:
        lines.append("Parity: OK (0 difference)")
    else:
        lines.append(
            f"WARNING: {db_postgres.ERR_PARITY_MISMATCH} — {delta} record(s) differ between sinks"
        )
    return "\n".join(lines)


def _surreal_filing_count() -> int:
    """Return the number of filing records in SurrealDB (0 on failure)."""
    result = surreal_query("SELECT count() FROM exchange_filing GROUP ALL;", timeout=60)
    if isinstance(result, list) and result:
        rows = result[0].get("result", [])
        if rows:
            return rows[0].get("count", 0) or 0
    return 0


def _fetch_surreal_coverage() -> list:
    """Return coverage rows from SurrealDB (0 rows on failure)."""
    result = surreal_query(
        "SELECT chunkFrom, chunkTo, apiCount, ingestedCount, uniqueCount, "
        "runId, timestamp FROM scrape_coverage ORDER BY chunkFrom DESC;",
        timeout=30,
    )
    if isinstance(result, list) and len(result) > 0:
        return result[0].get("result", [])
    return []


def _print_coverage(records: list, surreal: bool) -> None:
    """Print a coverage table from either sink's row shape."""
    if not records:
        log("No coverage data found. Run the scraper first to generate coverage stats.")
        return
    print(
        f"{'From':<12} {'To':<12} {'API':>6} {'Raw':>6} {'Unique':>6} "
        f"{'%':>6} {'Run ID':<18} {'Timestamp'}"
    )
    print("-" * 90)
    for r in records:
        if surreal:
            cf, ct = r.get("chunkFrom"), r.get("chunkTo")
            api = r.get("apiCount", 0) or 0
            ingested = r.get("ingestedCount", 0) or 0
            unique = r.get("uniqueCount", 0) or 0
            rid = r.get("runId", "-") or "-"
            ts = r.get("timestamp", "-") or "-"
        else:
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
            f"{cf_s:<12} {ct_s:<12} {api:>6} {ingested:>6} {unique:>6} "
            f"{pct:>5}% {str(rid):<18} {ts}"
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
            "into one or more database sinks (SurrealDB and/or PostgreSQL). Full "
            "pipeline runs by default: metadata scrape, document download, text "
            "extraction, and graph linking."
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
        help="Print historical coverage data from scrape_coverage table and exit",
    )
    parser.add_argument(
        "--database-target",
        type=str,
        default="",
        choices=["surrealdb", "postgres", "both"],
        metavar="TARGET",
        help="Override DATABASE_TARGET for this run (surrealdb | postgres | both)",
    )
    parser.add_argument(
        "--parity-report",
        action="store_true",
        help="Print per-sink filing counts and the difference, then exit",
    )
    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


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
        log(
            f"Database target: {config.DATABASE_TARGET} "
            f"(surrealdb={'on' if config.surrealdb_enabled() else 'off'}, "
            f"postgres={'on' if config.postgres_enabled() else 'off'})"
        )
        if config.postgres_enabled() and not db_postgres.postgres_available():
            log(
                "  WARNING: PostgreSQL sink enabled but unavailable "
                "(driver or connection details missing)"
            )

        if config.surrealdb_enabled():
            initialize_schema()
        if config.postgres_enabled() and db_postgres.postgres_available():
            pg_ok, pg_code = db_postgres.initialize_postgres_schema()
            if pg_ok:
                log("  PostgreSQL schema initialized successfully")
            else:
                log(f"  PostgreSQL schema init warning: {pg_code[:300]}")
                if config.postgres_required():
                    log("ERROR: PostgreSQL is the only configured sink and is not usable.")
                    sys.exit(1)

        if args.parity_report:
            log("PARITY REPORT")
            if not config.postgres_enabled():
                log(
                    "ERROR: --parity-report requires the PostgreSQL sink "
                    "(use --database-target both)."
                )
                sys.exit(1)
            if not db_postgres.postgres_available():
                log("ERROR: PostgreSQL sink unavailable (driver or connection details missing).")
                sys.exit(1)
            surreal_count = _surreal_filing_count() if config.surrealdb_enabled() else 0
            pg_count, pg_code = db_postgres.count_filings()
            if pg_code:
                log(f"ERROR: could not count PostgreSQL filings: {pg_code[:200]}")
                sys.exit(1)
            print(_format_parity_report(surreal_count, pg_count, config.surrealdb_enabled()))
            sys.exit(0)

        if args.coverage_report:
            log("COVERAGE REPORT")
            log("=" * 60)
            if config.surrealdb_enabled():
                _print_coverage(_fetch_surreal_coverage(), surreal=True)
            elif config.postgres_enabled() and db_postgres.postgres_available():
                records, cov_code = db_postgres.fetch_coverage()
                if cov_code:
                    log(f"ERROR: could not read coverage from PostgreSQL: {cov_code[:200]}")
                    sys.exit(1)
                _print_coverage(records, surreal=False)
            else:
                log("ERROR: no usable sink configured for the coverage report.")
                sys.exit(1)
            sys.exit(0)

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
            log("ERROR: one or more required sink writes failed (see sink summary above)")
        sys.exit(exit_code)
    finally:
        close_log_file()
        if config.postgres_enabled():
            db_postgres.close_pool()


if __name__ == "__main__":
    main()
