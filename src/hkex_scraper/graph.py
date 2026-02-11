"""Graph linking: create edges between companies and filings in SurrealDB."""

from __future__ import annotations

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


def _load_company_ids() -> set[str]:
    """Fetch all record IDs from the configured company table."""
    company_ids: set[str] = set()
    comp_result = surreal_query(f"SELECT id FROM {COMPANY_TABLE};", timeout=60)
    if isinstance(comp_result, list) and len(comp_result) > 0:
        for c in comp_result[0].get("result", []):
            cid = c.get("id", "")
            if cid:
                company_ids.add(str(cid))
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
    else:
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

    company_ids = _load_company_ids()

    valid_tickers: list[str] = []
    skipped = 0
    for ticker in tickers:
        record_id = _ticker_to_record_id(ticker)
        full_id = f"{COMPANY_TABLE}:{record_id}"
        if full_id in company_ids:
            valid_tickers.append(ticker)
        else:
            skipped += 1
    log(f"  Valid tickers: {len(valid_tickers)}, Skipped (no company): {skipped}")

    LINK_BATCH_SIZE = 50
    linked = 0
    errors = 0
    for batch_start in range(0, len(valid_tickers), LINK_BATCH_SIZE):
        batch = valid_tickers[batch_start : batch_start + LINK_BATCH_SIZE]
        sql_parts: list[str] = []
        for ticker in batch:
            safe_ticker = escape_sql(ticker)
            record_id = _ticker_to_record_id(ticker)
            sql_parts.append(
                f"LET $f_{record_id} = SELECT id FROM exchange_filing "
                f"WHERE companyTicker = '{safe_ticker}';\n"
                f"FOR $r IN $f_{record_id} {{\n"
                f"  RELATE ({COMPANY_TABLE}:{record_id})->has_filing->($r.id)\n"
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

    log(
        f"  Linking complete: {linked} linked, {skipped} skipped (no company), "
        f"{errors} errors"
    )
    count_result = surreal_query(
        "SELECT count() FROM has_filing GROUP ALL;", timeout=30
    )
    total = 0
    if isinstance(count_result, list) and len(count_result) > 0:
        r = count_result[0].get("result", [])
        if r and len(r) > 0:
            total = r[0].get("count", 0)
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
    while True:
        if ticker_set is not None:
            ticker_list = ", ".join(
                [f"'{t.replace(chr(39), chr(92) + chr(39))}'" for t in sorted(ticker_set)]
            )
            sql = (
                f"SELECT id, title, stockCode, companyTicker FROM exchange_filing "
                f"WHERE companyTicker IN [{ticker_list}] "
                f"ORDER BY id ASC START {offset} LIMIT {PAGE_SIZE};"
            )
        else:
            sql = (
                f"SELECT id, title, stockCode, companyTicker FROM exchange_filing "
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

    log(f"  Scanning {len(filings)} filings for cross-references")
    xrefs: list = []
    for f in filings:
        title = f.get("title", "") or ""
        stock_code = str(f.get("stockCode", "") or "")
        filing_id = f.get("id", "")
        if not title or not filing_id:
            continue
        refs = extract_referenced_tickers(title, stock_code)
        for ref_ticker in refs:
            xrefs.append((filing_id, ref_ticker))

    log(
        f"  Found {len(xrefs)} cross-references across "
        f"{len(set(x[0] for x in xrefs))} filings"
    )
    if not xrefs:
        return 0

    company_ids = _load_company_ids()

    valid_xrefs: list = []
    skipped = 0
    for filing_id, ref_ticker in xrefs:
        record_id = _ticker_to_record_id(ref_ticker)
        full_id = f"{COMPANY_TABLE}:{record_id}"
        if full_id in company_ids:
            valid_xrefs.append((filing_id, record_id))
        else:
            skipped += 1
    log(f"  Valid cross-refs: {len(valid_xrefs)}, Skipped (no company): {skipped}")

    XREF_BATCH_SIZE = 100
    created = 0
    errors = 0
    for batch_start in range(0, len(valid_xrefs), XREF_BATCH_SIZE):
        batch = valid_xrefs[batch_start : batch_start + XREF_BATCH_SIZE]
        sql_parts: list[str] = []
        for filing_id, record_id in batch:
            sql_parts.append(
                f"RELATE ({filing_id})->references_filing->({COMPANY_TABLE}:{record_id})"
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
        if processed % 500 == 0 or processed == len(valid_xrefs):
            log(
                f"  Processed {processed}/{len(valid_xrefs)} cross-refs "
                f"({created} created, {errors} errors)"
            )

    log(
        f"  Cross-referencing complete: {created} edges created, "
        f"{skipped} skipped (no company), {errors} errors"
    )
    return created
