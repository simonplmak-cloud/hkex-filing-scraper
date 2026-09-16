"""Graph linking: dispatch company/file edges to every edge-capable sink.

The linker is sink-agnostic: it builds canonical edges (company ticker + filing
id) once and hands them to every configured sink that declares edge support.
SurrealDB resolves its own company record ids and issues ``RELATE``; relational
sinks derive the company key from the ticker.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from . import pipeline, sinks
from .config import COMPANY_TABLE
from .utils import extract_referenced_tickers, log

PAGE_SIZE = 5000


def _read_tickers(reader, ticker_set: Optional[set]) -> Optional[List[str]]:
    """Return the ticker set to link (from the current run or a full scan)."""
    if ticker_set is not None:
        log(f"  Linking {len(ticker_set)} tickers from current run")
        return sorted(ticker_set)
    tickers, err = reader.distinct_company_tickers()
    if err:
        log(f"  Failed to get tickers via {reader.id}: {err[:200]}")
        return None
    log(f"  Found {len(tickers)} distinct tickers to link (full scan)")
    return tickers


def _filing_pairs(reader, tickers: List[str]) -> Tuple[List[Dict[str, Any]], str]:
    """Return canonical ``{company_ticker, filing_id}`` pairs for *tickers*."""
    if not tickers:
        return [], ""
    rows, err = reader.fetch_filing_ids_by_ticker(tickers)
    if err:
        return [], err
    edges = [
        {"company_ticker": r.get("company_ticker", ""), "filing_id": r.get("filing_id", "")}
        for r in rows
        if r.get("company_ticker") and r.get("filing_id")
    ]
    return edges, ""


def _dispatch_edges(edges: List[Dict[str, Any]], kind: str) -> int:
    """Write *edges* to every edge-capable sink; return the largest created count."""
    if not edges:
        return 0
    total_created = 0
    for sink in sinks.edge_sinks():
        if not sink.available():
            pipeline.warn_sink_unavailable_once(sink)
            continue
        try:
            created, err_code = sink.upsert_edges(edges, kind)
        except Exception as exc:  # noqa: BLE001 - one sink must not abort another
            pipeline.record_sink(sink.id, False, len(edges))
            log(f"  {sink.id} {kind} edges raised: {type(exc).__name__}: {exc}")
            continue
        if err_code:
            pipeline.record_sink(sink.id, False, len(edges) or 1)
            log(f"  {sink.id} {kind} edges failed: {err_code[:200]}")
        else:
            pipeline.record_sink(sink.id, True, created)
            log(f"  {sink.id} {kind} edges inserted: {created}")
        total_created = max(total_created, created)
    return total_created


# ---------------------------------------------------------------------------
# has_filing edges
# ---------------------------------------------------------------------------


def link_filings_to_companies(ticker_set: set | None = None) -> int:
    """Link filings to the company table via ``has_filing`` edges on every sink.

    If *ticker_set* is provided, only those tickers are processed (incremental).
    Otherwise all distinct tickers in the read source are scanned.
    """
    if not COMPANY_TABLE:
        log("  Graph linking disabled (COMPANY_TABLE not set). Skipping.")
        return 0

    log("Linking filings to companies via graph edges...")

    reader = sinks.read_sink()
    if reader is None:
        log("  No configured sink supports reads; graph linking skipped.")
        return 0

    tickers = _read_tickers(reader, ticker_set)
    if tickers is None:
        return 0

    edges, err = _filing_pairs(reader, tickers)
    if err:
        log(f"  Failed to resolve filings via {reader.id}: {err[:200]}")
        return 0
    log(f"  {len(edges)} filing/ticker pairs to link")

    _dispatch_edges(edges, "has_filing")

    total = 0
    if reader.available():
        total, _code = reader.count_edges("has_filing")
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

    reader = sinks.read_sink()
    if reader is None:
        log("  No configured sink supports reads; cross-referencing skipped.")
        return 0

    ticker_arg = sorted(ticker_set) if ticker_set is not None else None
    filings: list = []
    offset = 0
    while True:
        batch, page_code = reader.fetch_titles(ticker_arg, offset, PAGE_SIZE)
        if page_code:
            log(f"  Failed to get filings via {reader.id} (offset {offset}): {page_code[:200]}")
            break
        if not batch:
            break
        filings.extend(batch)
        log(f"  Fetched {len(filings)} filings so far (batch of {len(batch)})")
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    log(f"  Scanning {len(filings)} filings for cross-references")

    seen: set = set()
    xrefs: List[Dict[str, Any]] = []
    for f in filings:
        title = f.get("title", "") or ""
        stock_code = str(f.get("stock_code", "") or "")
        filing_id = f.get("filing_id", "")
        if not title or not filing_id:
            continue
        for ref_ticker in extract_referenced_tickers(title, stock_code):
            key = (filing_id, ref_ticker)
            if key in seen:
                continue
            seen.add(key)
            xrefs.append(
                {
                    "filing_id": filing_id,
                    "company_ticker": ref_ticker,
                    "source": "title_extraction",
                }
            )

    filing_count = len(set(x["filing_id"] for x in xrefs))
    log(f"  Found {len(xrefs)} cross-references across {filing_count} filings")
    created = _dispatch_edges(xrefs, "references_filing")
    log(f"  Cross-referencing complete: {created} edges created")
    return created
