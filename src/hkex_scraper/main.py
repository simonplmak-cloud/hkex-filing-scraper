"""CLI entry point and argument parsing."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime

from .config import LOG_DIR, MAX_DOWNLOAD_WORKERS, SURREAL_ENDPOINT, SURREAL_PASS
from .db import initialize_schema
from .extractor import check_dependencies
from .graph import cross_reference_filings, link_filings_to_companies
from .pipeline import run_phase1, run_phase2
from .utils import close_log_file, log, set_log_file


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _validate_env() -> None:
    """Ensure minimum required environment variables are set."""
    missing: list[str] = []
    if not SURREAL_ENDPOINT:
        missing.append("SURREAL_ENDPOINT")
    if not SURREAL_PASS:
        missing.append("SURREAL_PASSWORD")
    if missing:
        log(f"ERROR: Missing required env vars: {', '.join(missing)}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hkex-scraper",
        description=(
            "Scrape and ingest HKEx (Hong Kong Stock Exchange) regulatory filings "
            "into SurrealDB. Full pipeline runs by default: metadata scrape, "
            "document download, text extraction, and graph linking."
        ),
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
    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Entry point for the ``hkex-scraper`` CLI."""
    parser = _build_parser()
    args = parser.parse_args()

    check_dependencies()
    _validate_env()

    LOG_DIR.mkdir(exist_ok=True)
    log_path = LOG_DIR / f"hkex_filings_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.log"
    set_log_file(open(log_path, "w", encoding="utf-8"))

    try:
        initialize_schema()

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

        log("=" * 60)
        log("COMPLETE")
        log("=" * 60)
        log(f"Log: {log_path}")
    finally:
        close_log_file()


if __name__ == "__main__":
    main()
