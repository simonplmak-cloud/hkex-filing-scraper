"""Graph linking: create edges between companies and filings in SurrealDB."""

from __future__ import annotations

from . import config, db_postgres, pipeline
from .config import COMPANY_ID_PATTERN, COMPANY_TABLE
from .db import surreal_query
from .utils import escape_sql, extract_referenced_tickers, log

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ticker_to_record_id(ticker: str) -> str:
    """Convert a ticker (e.g. ``0451.HK``) to a record ID using the configured pattern.

    Default pattern ``{code}_{exchange}`` produces ``451_HK``.
    """
    parts = ticker.split(".")
    code = parts[0].lstrip("0") or "0"
    exchange = parts[1] if len(parts) > 1 else "HK"
    return COMPANY_ID_PATTERN.format(code=code, exchange=exchange)


def _normalize_company_id(full_id: str) -> str:
    """Normalize a DB record ID for matching (strip leading zeros from the code segment)."""
    if ":" in full_id:
        tail = full_id.rsplit(":", 1)[-1]
        parts = tail.split("_", 1)
        code = parts[0].lstrip("0") or "0"
        exchange = parts[1] if len(parts) > 1 else ""
        return f"{code}_{exchange}"
    return full_id


def _load_company_ids() -> dict[str, str]:
    """Fetch all record IDs from the configured company table.

    Returns a dict mapping normalized key → actual DB record ID. A missing or
    unreadable company table is logged and treated as an empty set (graph
    linking is skipped) rather than crashing the pipeline.
    """
    company_ids: dict[str, str] = {}
    comp_result = surreal_query(f"SELECT id FROM {COMPANY_TABLE};", timeout=60)
    if isinstance(comp_result, dict) and comp_result.get("error"):
        log(f"  Could not load company IDs: {str(comp_result['error'])[:200]}")
        return company_ids
    if isinstance(comp_result, list) and len(comp_result) > 0:
        entry = comp_result[0]
        rows = entry.get("result", []) if isinstance(entry, dict) else []
        if not isinstance(rows, list):
            log(
                f"  Could not read company table '{COMPANY_TABLE}': "
                f"{str(rows)[:200]} (graph linking skipped)"
            )
            return company_ids
        for c in rows:
            if not isinstance(c, dict):
                continue
            cid = str(c.get("id", ""))
            if cid:
                company_ids[_normalize_company_id(cid)] = cid
    log(f"  Loaded {len(company_ids)} company IDs for matching")
    return company_ids


# ---------------------------------------------------------------------------
# has_filing edges
# ---------------------------------------------------------------------------


def link_filings_to_companies(ticker_set: set | None = None) -> int:
    """Link ``exchange_filing`` records to the company table via ``has_filing`` edges.

    If *ticker_set* is provided, only those tickers are processed (incremental).
    Otherwise all distinct tickers in the filing table are scanned.
    """
    if not COMPANY_TABLE:
        log("  Graph linking disabled (COMPANY_TABLE not set). Skipping.")
        return 0

    log("Linking filings to companies via graph edges...")

    if ticker_set is not None:
        tickers = sorted(ticker_set)
        log(f"  Linking {len(tickers)} tickers from current run")
    elif config.surrealdb_enabled():
        ticker_result = surreal_query(
            "SELECT companyTicker FROM exchange_filing "
            "WHERE companyTicker IS NOT NONE "
            "GROUP BY companyTicker;",
            timeout=120,
        )
        if isinstance(ticker_result, dict) and ticker_result.get("error"):
            log(f"  Failed to get tickers: {ticker_result['error'][:200]}")
            return 0
        tickers = []
        if isinstance(ticker_result, list) and len(ticker_result) > 0:
            r = ticker_result[0].get("result", [])
            tickers = [x.get("companyTicker", "") for x in r if x.get("companyTicker")]
        log(f"  Found {len(tickers)} distinct tickers to link (full scan)")
    else:
        tickers, ticker_code = db_postgres.distinct_company_tickers()
        if ticker_code:
            log(f"  Failed to get tickers from PostgreSQL: {ticker_code[:200]}")
            return 0
        log(f"  Found {len(tickers)} distinct tickers to link (PostgreSQL full scan)")

    company_ids: dict[str, str] = {}
    if config.surrealdb_enabled():
        company_ids = _load_company_ids()

    valid_tickers: list[tuple[str, str]] = []
    skipped = 0
    for ticker in tickers:
        record_id = _ticker_to_record_id(ticker)
        db_id = company_ids.get(record_id)
        if db_id:
            valid_tickers.append((ticker, db_id))
        elif not config.surrealdb_enabled():
            # PostgreSQL has no company FK; the normalised id is the company key.
            valid_tickers.append((ticker, record_id))
        else:
            skipped += 1
    log(f"  Valid tickers: {len(valid_tickers)}, Skipped (no company): {skipped}")

    LINK_BATCH_SIZE = 50
    linked = 0
    errors = 0
    if config.surrealdb_enabled():
        for batch_start in range(0, len(valid_tickers), LINK_BATCH_SIZE):
            batch = valid_tickers[batch_start : batch_start + LINK_BATCH_SIZE]
            sql_parts: list[str] = []
            for ticker, db_id in batch:
                safe_ticker = escape_sql(ticker)
                record_id = _ticker_to_record_id(ticker)
                sql_parts.append(
                    f"LET $f_{record_id} = SELECT id FROM exchange_filing "
                    f"WHERE companyTicker = '{safe_ticker}';\n"
                    f"FOR $r IN $f_{record_id} {{\n"
                    f"  RELATE ({db_id})->has_filing->($r.id)\n"
                    f"    SET createdAt = time::now()\n"
                    f"    RETURN NONE;\n"
                    f"}};\n"
                )
            batch_sql = "\n".join(sql_parts)
            result = surreal_query(batch_sql, timeout=300)
            if isinstance(result, dict) and result.get("error"):
                log(f"  Batch error at offset {batch_start}: {result['error'][:200]}")
                errors += len(batch)
            else:
                linked += len(batch)
            processed = batch_start + len(batch)
            if processed % 200 == 0 or processed == len(valid_tickers):
                log(
                    f"  Processed {processed}/{len(valid_tickers)} tickers "
                    f"({linked} linked, {errors} errors)"
                )
    else:
        log("  SurrealDB disabled; skipping RELATE (PostgreSQL edges below)")

    if config.postgres_enabled():
        if not db_postgres.postgres_available():
            pipeline.warn_pg_unavailable_once()
        else:
            ticker_list = [t for t, _ in valid_tickers]
            rows, fetch_code = db_postgres.fetch_filing_ids_by_ticker(ticker_list)
            if fetch_code:
                log(f"  PostgreSQL ticker lookup failed: {fetch_code[:200]}")
                pipeline.record_sink("postgres", False, len(valid_tickers) or 1)
            else:
                pg_edges = [
                    {
                        "company_id": _ticker_to_record_id(r.get("company_ticker", "")),
                        "filing_id": r.get("filing_id", ""),
                    }
                    for r in rows
                    if r.get("company_ticker") and r.get("filing_id")
                ]
                created, edge_code = db_postgres.upsert_edges(pg_edges, "has_filing")
                if edge_code:
                    pipeline.record_sink("postgres", False, len(pg_edges) or 1)
                    log(f"  PostgreSQL has_filing edges failed: {edge_code[:200]}")
                else:
                    pipeline.record_sink("postgres", True, created)
                    log(f"  PostgreSQL has_filing edges inserted: {created}")

    log(f"  Linking complete: {linked} linked, {skipped} skipped (no company), {errors} errors")
    total = linked
    if config.surrealdb_enabled():
        count_result = surreal_query("SELECT count() FROM has_filing GROUP ALL;", timeout=30)
        if isinstance(count_result, list) and len(count_result) > 0:
            r = count_result[0].get("result", [])
            if r and len(r) > 0:
                total = r[0].get("count", 0)
    elif config.postgres_enabled():
        total, _code = db_postgres.count_edges("has_filing")
    log(f"  Total has_filing edges: {total}")
    return total


# ---------------------------------------------------------------------------
# references_filing edges (cross-references)
# ---------------------------------------------------------------------------


def cross_reference_filings(ticker_set: set | None = None) -> int:
    """Create ``references_filing`` edges for companies mentioned in filing titles."""
    if not COMPANY_TABLE:
        log("  Cross-referencing disabled (COMPANY_TABLE not set). Skipping.")
        return 0

    log("Cross-referencing filings to mentioned companies...")

    PAGE_SIZE = 5000
    filings: list = []
    offset = 0
    if config.surrealdb_enabled():
        while True:
            if ticker_set is not None:
                ticker_list = ", ".join(
                    [f"'{t.replace(chr(39), chr(92) + chr(39))}'" for t in sorted(ticker_set)]
                )
                sql = (
                    f"SELECT id, filingId, title, stockCode, companyTicker "
                    f"FROM exchange_filing "
                    f"WHERE companyTicker IN [{ticker_list}] "
                    f"ORDER BY id ASC START {offset} LIMIT {PAGE_SIZE};"
                )
            else:
                sql = (
                    f"SELECT id, filingId, title, stockCode, companyTicker "
                    f"FROM exchange_filing "
                    f"WHERE title IS NOT NONE "
                    f"ORDER BY id ASC START {offset} LIMIT {PAGE_SIZE};"
                )
            result = surreal_query(sql, timeout=300)
            if isinstance(result, dict) and result.get("error"):
                log(f"  Failed to get filings (offset {offset}): {result['error'][:200]}")
                break
            batch: list = []
            if isinstance(result, list) and len(result) > 0:
                batch = result[0].get("result", [])
            if not batch:
                break
            filings.extend(batch)
            log(f"  Fetched {len(filings)} filings so far (batch of {len(batch)})")
            if len(batch) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
    else:
        ticker_arg = sorted(ticker_set) if ticker_set is not None else None
        while True:
            batch, page_code = db_postgres.fetch_titles(ticker_arg, offset, PAGE_SIZE)
            if page_code:
                log(f"  Failed to get filings from PostgreSQL (offset {offset}): {page_code[:200]}")
                break
            if not batch:
                break
            filings.extend(batch)
            log(f"  Fetched {len(filings)} filings so far (batch of {len(batch)})")
            if len(batch) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

    log(f"  Scanning {len(filings)} filings for cross-references")
    # Each entry: (sink_ref, filing_id, ref_ticker) where sink_ref is the SurrealDB
    # record id (when present) and filing_id is the shared MD5 key.
    xrefs: list = []
    for f in filings:
        title = f.get("title", "") or ""
        stock_code = str(f.get("stockCode", f.get("stock_code", "")) or "")
        filing_ref = f.get("id") or f.get("filing_id") or ""
        filing_id = f.get("filingId") or f.get("filing_id") or ""
        if not title or not filing_id:
            continue
        for ref_ticker in extract_referenced_tickers(title, stock_code):
            xrefs.append((filing_ref, filing_id, ref_ticker))

    log(f"  Found {len(xrefs)} cross-references across {len(set(x[1] for x in xrefs))} filings")
    if not xrefs:
        return 0

    company_ids: dict[str, str] = {}
    if config.surrealdb_enabled():
        company_ids = _load_company_ids()

    surreal_xrefs: list = []
    pg_xrefs: list = []
    skipped = 0
    for filing_ref, filing_id, ref_ticker in xrefs:
        record_id = _ticker_to_record_id(ref_ticker)
        db_id = company_ids.get(record_id)
        if db_id:
            surreal_xrefs.append((filing_ref, db_id))
            pg_xrefs.append({"filing_id": filing_id, "company_id": record_id})
        elif not config.surrealdb_enabled():
            pg_xrefs.append({"filing_id": filing_id, "company_id": record_id})
        else:
            skipped += 1
    valid_count = len(surreal_xrefs) if config.surrealdb_enabled() else len(pg_xrefs)
    log(f"  Valid cross-refs: {valid_count}, Skipped (no company): {skipped}")

    XREF_BATCH_SIZE = 100
    created = 0
    errors = 0
    if config.surrealdb_enabled():
        for batch_start in range(0, len(surreal_xrefs), XREF_BATCH_SIZE):
            batch = surreal_xrefs[batch_start : batch_start + XREF_BATCH_SIZE]
            sql_parts: list[str] = []
            for filing_ref, db_id in batch:
                sql_parts.append(
                    f"RELATE ({filing_ref})->references_filing->({db_id})"
                    f" SET createdAt = time::now(), source = 'title_extraction'"
                    f" RETURN NONE;"
                )
            batch_sql = "\n".join(sql_parts)
            result = surreal_query(batch_sql, timeout=120)
            if isinstance(result, dict) and result.get("error"):
                log(f"  Batch error at offset {batch_start}: {result['error'][:200]}")
                errors += len(batch)
            else:
                created += len(batch)
            processed = batch_start + len(batch)
            if processed % 500 == 0 or processed == len(surreal_xrefs):
                log(
                    f"  Processed {processed}/{len(surreal_xrefs)} cross-refs "
                    f"({created} created, {errors} errors)"
                )
    else:
        log("  SurrealDB disabled; skipping RELATE (PostgreSQL edges below)")

    pg_created = 0
    if config.postgres_enabled():
        if not db_postgres.postgres_available():
            pipeline.warn_pg_unavailable_once()
        else:
            pg_created, pg_code = db_postgres.upsert_edges(pg_xrefs, "references_filing")
            if pg_code:
                pipeline.record_sink("postgres", False, len(pg_xrefs) or 1)
                log(f"  PostgreSQL references_filing edges failed: {pg_code[:200]}")
            else:
                pipeline.record_sink("postgres", True, pg_created)
                log(f"  PostgreSQL references_filing edges inserted: {pg_created}")

    total_created = created if config.surrealdb_enabled() else pg_created
    log(
        f"  Cross-referencing complete: {total_created} edges created, "
        f"{skipped} skipped (no company), {errors} errors"
    )
    return total_created
