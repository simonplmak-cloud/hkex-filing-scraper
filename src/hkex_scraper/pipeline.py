"""Pipeline orchestration: Phase 1 (metadata scrape) and Phase 2 (document backfill)."""

from __future__ import annotations

import hashlib
import io
import json
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Tuple

from .api import REQUESTS_AVAILABLE, fetch_chunk_via_api, generate_monthly_chunks
from .config import HKEX_BASE_URL, MAX_DOWNLOAD_SIZE, MAX_DOWNLOAD_WORKERS, MAX_SQL_BODY_SIZE
from .db import surreal_query, upsert_batch_with_retry
from .extractor import extract_content_with_tables
from .utils import (
    classify_filing,
    escape_sql,
    extract_issuer_name,
    extract_referenced_tickers,
    is_derivative_issuer_filing,
    log,
    squash_ws,
)

# ---------------------------------------------------------------------------
# Supported document extensions
# ---------------------------------------------------------------------------
SUPPORTED_EXTENSIONS = (".pdf", ".htm", ".html", ".xlsx", ".xls", ".doc", ".docx")


# ---------------------------------------------------------------------------
# Document download helpers
# ---------------------------------------------------------------------------

def _download_document(url: str, filing_id: str) -> Tuple[bytes, int, str]:
    """Download a document if <= MAX_DOWNLOAD_SIZE.

    Returns ``(raw_bytes, size_bytes, skip_reason)``.
    """
    if not url:
        return b"", 0, "no_url"
    u = url.lower().split("?")[0].split("#")[0]
    if not any(u.endswith(ext) for ext in SUPPORTED_EXTENSIONS):
        return b"", 0, "unsupported_type"
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": (
                    "application/pdf,text/html,"
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,"
                    "*/*;q=0.8"
                ),
            },
        )
        with urllib.request.urlopen(req, timeout=60) as response:
            content_length = response.headers.get("Content-Length")
            if content_length and int(content_length) > MAX_DOWNLOAD_SIZE:
                return b"", 0, "too_large"
            content = response.read()
            if len(content) > MAX_DOWNLOAD_SIZE:
                return b"", 0, "too_large"
            return content, len(content), ""
    except urllib.error.HTTPError as e:
        log(f"  Download error for {filing_id}: HTTP {e.code}")
        return b"", 0, f"http_{e.code}"
    except Exception as e:
        log(f"  Download error for {filing_id}: {type(e).__name__}: {e}")
        return b"", 0, f"error:{type(e).__name__}"


def _download_worker(args: Tuple[str, str]) -> Tuple[str, str, bytes, int, str]:
    """Thread-safe wrapper for ``_download_document``."""
    fid, url = args
    raw_bytes, size_bytes, skip_reason = _download_document(url, fid)
    return fid, url, raw_bytes, size_bytes, skip_reason


# ---------------------------------------------------------------------------
# Save helpers
# ---------------------------------------------------------------------------

def _save_filings_batch_metadata(filings: list, dry_run: bool = False) -> int:
    """Save filing metadata in batches. Phase 1 operation."""
    if not filings:
        return 0
    if dry_run:
        log(f" [DRY-RUN] Would save {len(filings)} filings (metadata)")
        return len(filings)

    sql_statements: List[str] = []
    for f in filings:
        fid = hashlib.md5(
            f"{f['stockCode']}{f['date']}{f.get('title', '')}".encode()
        ).hexdigest()[:16]

        title_str = f.get("title", "")
        ft, fs = classify_filing(title_str)
        date_str = f.get("date", "")
        filing_date_expr = "NULL"
        if date_str:
            try:
                dd, mm, yyyy = date_str.split("/")
                filing_date_expr = f"d'{yyyy}-{mm}-{dd}'"
            except Exception:
                filing_date_expr = "NULL"

        raw_code = str(f["stockCode"]).lstrip("0") or "0"

        # Detect derivative issuer filings (empty stock code + matching title)
        filing_category = "LISTED_COMPANY"  # default
        if (not f["stockCode"].strip()) and is_derivative_issuer_filing(title_str):
            issuer_short = extract_issuer_name(title_str)
            ticker = f"{issuer_short}_DERIV.HK"
            filing_category = "DERIVATIVE_ISSUER"
        elif not f["stockCode"].strip():
            ticker = "UNKNOWN.HK"
            filing_category = "UNKNOWN"
        else:
            ticker = f"{raw_code.zfill(4)}.HK"

        doc_url = f.get("link", "")

        ref_tickers = extract_referenced_tickers(title_str, str(f["stockCode"]))
        ref_tickers_json = json.dumps(ref_tickers)

        sql_statements.append(
            "UPSERT exchange_filing:{fid} SET\n"
            "  filingId       = '{fid}',\n"
            "  companyTicker  = '{ticker}',\n"
            "  stockCode      = '{stockCode}',\n"
            "  stockName      = '{stockName}',\n"
            "  exchange       = 'HK',\n"
            "  filingType     = '{ft}',\n"
            "  filingSubtype  = '{fs}',\n"
            "  filingCategory = '{filingCategory}',\n"
            "  title          = '{title}',\n"
            "  filingDate     = {filingDateExpr},\n"
            "  documentUrl    = '{docUrl}',\n"
            "  referencedTickers = {refTickers},\n"
            "  source         = 'HKEx',\n"
            "  updatedAt      = time::now()\n"
            "RETURN NONE;\n".format(
                fid=fid,
                ticker=ticker,
                stockCode=escape_sql(f["stockCode"]),
                stockName=escape_sql(squash_ws(f.get("stockName", ""))),
                ft=ft,
                fs=escape_sql(fs),
                filingCategory=filing_category,
                title=escape_sql(squash_ws(title_str)),
                filingDateExpr=filing_date_expr,
                docUrl=escape_sql(doc_url),
                refTickers=ref_tickers_json,
            )
        )

    saved_count = upsert_batch_with_retry(sql_statements)
    if saved_count < len(sql_statements):
        log(f"  Saved {saved_count} / {len(sql_statements)} filings after retries")
    return saved_count


def _save_document_to_filing(
    fid: str,
    raw_bytes: bytes,
    size_bytes: int,
    doc_url: str,
    extracted_text: str = "",
    tables_json: list | None = None,
) -> Tuple[bool, str]:
    """Save extracted text + metadata to an existing filing record.

    Proactively truncates the SQL payload to stay under the SurrealDB Cloud
    ``/sql`` endpoint body limit (~1 MiB).  This prevents both HTTP 413
    responses AND lower-level TCP connection resets (WinError 10053 /
    ECONNRESET) that occur when the payload is so large the server drops
    the connection before it can even return an error code.
    """
    ext = doc_url.lower().split("?")[0].split("#")[0]
    if ext.endswith(".pdf"):
        doc_type = "pdf"
    elif ext.endswith(".htm") or ext.endswith(".html"):
        doc_type = "html"
    elif ext.endswith(".xlsx") or ext.endswith(".xls"):
        doc_type = "xlsx"
    elif ext.endswith(".doc") or ext.endswith(".docx"):
        doc_type = "docx"
    else:
        doc_type = "unknown"

    doc_hash = hashlib.md5(raw_bytes).hexdigest() if raw_bytes else ""
    tables_count = len(tables_json) if tables_json else 0
    native_tables = json.dumps(tables_json, ensure_ascii=False) if tables_json else "[]"

    # --- Pre-truncation: estimate SQL payload size and shrink BEFORE sending ---
    # The SurrealDB Cloud /sql endpoint has a hard 1 MiB body limit.
    # We target 900 KB to leave headroom for SQL boilerplate + escaping overhead.
    SQL_OVERHEAD_ESTIMATE = 1024  # bytes for the UPDATE ... SET ... wrapper
    tables_payload_size = len(native_tables.encode("utf-8"))
    text_to_save = extracted_text
    was_truncated = False
    original_text_len = len(extracted_text)

    # Estimate total payload: escaped text + tables JSON + SQL overhead
    # escape_sql can expand text (backslash doubling, quote escaping) so we
    # use a 1.1x safety multiplier on text size.
    estimated_text_bytes = int(len(text_to_save) * 1.1)
    estimated_total = estimated_text_bytes + tables_payload_size + SQL_OVERHEAD_ESTIMATE

    if estimated_total > MAX_SQL_BODY_SIZE:
        # First: try truncating text only (keep all tables)
        available_for_text = MAX_SQL_BODY_SIZE - tables_payload_size - SQL_OVERHEAD_ESTIMATE
        if available_for_text > 50_000:  # at least 50 KB of text is useful
            target_chars = int(available_for_text / 1.1)  # reverse the safety multiplier
            text_to_save = extracted_text[:target_chars]
            # Trim to last complete line
            last_nl = text_to_save.rfind("\n")
            if last_nl > target_chars // 2:
                text_to_save = text_to_save[:last_nl]
            was_truncated = True
            log(
                f"  Pre-truncated text for {fid}: {original_text_len} -> {len(text_to_save)} chars "
                f"(tables: {tables_payload_size} bytes, {tables_count} tables kept)"
            )
        else:
            # Tables JSON alone is too large — truncate tables too
            # Keep only the first N tables that fit in ~200 KB
            max_tables_bytes = 200 * 1024
            kept_tables: list = []
            running_size = 2  # for '[]'
            for tbl in tables_json or []:
                tbl_json = json.dumps(tbl, ensure_ascii=False)
                if running_size + len(tbl_json.encode("utf-8")) + 2 > max_tables_bytes:
                    break
                kept_tables.append(tbl)
                running_size += len(tbl_json.encode("utf-8")) + 2  # +2 for comma+space
            native_tables = json.dumps(kept_tables, ensure_ascii=False)
            tables_payload_size = len(native_tables.encode("utf-8"))
            tables_count = len(kept_tables)

            available_for_text = MAX_SQL_BODY_SIZE - tables_payload_size - SQL_OVERHEAD_ESTIMATE
            target_chars = max(int(available_for_text / 1.1), 50_000)
            text_to_save = extracted_text[:target_chars]
            last_nl = text_to_save.rfind("\n")
            if last_nl > target_chars // 2:
                text_to_save = text_to_save[:last_nl]
            was_truncated = True
            log(
                f"  Pre-truncated text+tables for {fid}: "
                f"text {original_text_len} -> {len(text_to_save)} chars, "
                f"tables {len(tables_json or [])} -> {tables_count}"
            )

    def _build_save_sql(
        text: str,
        tables_str: str,
        tbl_count: int,
        status: str = "processed",
        truncated: bool = False,
    ) -> str:
        safe_text = escape_sql(text) if text else ""
        text_len = len(text)
        reason = f"truncated_from_{original_text_len}" if truncated else ""
        return (
            "UPDATE exchange_filing:{fid} SET\n"
            "  documentSize     = {size},\n"
            "  documentType     = '{dtype}',\n"
            "  documentHash     = '{dh}',\n"
            "  documentText     = '{dt}',\n"
            "  documentTextLen  = {dtl},\n"
            "  documentTables   = {tables},\n"
            "  documentTableCnt = {tcnt},\n"
            "  documentStatus   = '{status}',\n"
            "  documentStatusReason = '{reason}',\n"
            "  updatedAt        = time::now()\n"
            "RETURN NONE;\n"
        ).format(
            fid=fid,
            size=size_bytes,
            dtype=doc_type,
            dh=doc_hash,
            dt=safe_text,
            dtl=text_len,
            tables=tables_str,
            tcnt=tbl_count,
            status=status,
            reason=reason,
        )

    def _is_413(result: dict) -> bool:
        """Detect body-too-large errors including connection resets."""
        if not isinstance(result, dict):
            return False
        err = str(result.get("error", ""))
        # Explicit HTTP 413
        if "413" in err:
            return True
        # TCP-level connection resets caused by oversized payloads
        if any(
            sig in err
            for sig in [
                "10053", "10054", "ECONNRESET", "Connection aborted",
                "Connection reset", "RemoteDisconnected", "BrokenPipeError",
            ]
        ):
            return True
        return False

    # --- First attempt ---
    sql = _build_save_sql(text_to_save, native_tables, tables_count, truncated=was_truncated)
    # Final safety check on actual encoded size
    actual_size = len(sql.encode("utf-8"))
    if actual_size > 1_048_576:  # 1 MiB hard limit
        # Emergency truncation — cut text more aggressively
        emergency_target = int(len(text_to_save) * (MAX_SQL_BODY_SIZE / actual_size) * 0.85)
        text_to_save = text_to_save[:emergency_target]
        last_nl = text_to_save.rfind("\n")
        if last_nl > emergency_target // 2:
            text_to_save = text_to_save[:last_nl]
        was_truncated = True
        log(f"  Emergency re-truncation for {fid}: -> {len(text_to_save)} chars (SQL was {actual_size} bytes)")
        sql = _build_save_sql(text_to_save, native_tables, tables_count, truncated=True)

    result = surreal_query(sql, timeout=60)
    if not (isinstance(result, dict) and result.get("error")):
        return True, ""  # success

    err = result.get("error", "unknown") if isinstance(result, dict) else "unknown"

    # --- 413 / connection-reset detected: retry with more aggressive truncation ---
    if _is_413(result):
        # Cut to 500 KB of text, drop tables entirely
        fallback_limit = 500 * 1024
        truncated_text = extracted_text[:fallback_limit]
        last_nl = truncated_text.rfind("\n")
        if last_nl > fallback_limit // 2:
            truncated_text = truncated_text[:last_nl]
        log(
            f"  Payload too large for {fid} ({str(err)[:80]}): "
            f"retrying with {len(truncated_text)} chars, no tables"
        )

        sql_retry = _build_save_sql(truncated_text, "[]", 0, truncated=True)
        result_retry = surreal_query(sql_retry, timeout=60)
        if not (isinstance(result_retry, dict) and result_retry.get("error")):
            return True, ""  # success with truncation

        # Truncation retry also failed
        retry_err = (
            result_retry.get("error", "unknown")
            if isinstance(result_retry, dict)
            else "unknown"
        )
        log(f"  Doc save failed for {fid} even after truncation: {str(retry_err)[:200]}")
        return False, "http_413_truncation_failed"

    # --- Non-413 error ---
    log(f"  Doc save failed for {fid}: {str(err)[:200]}")
    error_code = f"save_error:{str(err)[:100]}"
    return False, error_code


def _mark_filing_status(fid: str, status: str, reason: str = "") -> bool:
    """Mark a filing with a ``documentStatus`` (e.g. ``skipped``, ``failed``)."""
    safe_reason = escape_sql(reason[:200]) if reason else ""
    sql = (
        "UPDATE exchange_filing:{fid} SET\n"
        "  documentStatus = '{status}',\n"
        "  documentStatusReason = '{reason}',\n"
        "  updatedAt = time::now()\n"
        "RETURN NONE;\n"
    ).format(fid=fid, status=escape_sql(status), reason=safe_reason)
    result = surreal_query(sql, timeout=30)
    return not (isinstance(result, dict) and result.get("error"))


# ---------------------------------------------------------------------------
# Phase 1: Metadata scrape
# ---------------------------------------------------------------------------

def run_phase1(
    max_filings: int = 0,
    dry_run: bool = False,
    date_from: str = "",
    date_to: str = "",
    full_history: bool = False,
) -> Tuple[int, set]:
    """Scrape HKEx filings using the JSON API (no browser needed)."""
    if not REQUESTS_AVAILABLE:
        log("ERROR: 'requests' library not installed. Run: pip install requests")
        return 0, set()

    import requests as _requests

    today = datetime.now()
    if full_history:
        dt_from = datetime(1999, 4, 1)
        dt_to = today
    elif date_from and date_to:
        dt_from = datetime.strptime(date_from, "%d/%m/%Y")
        dt_to = datetime.strptime(date_to, "%d/%m/%Y")
    else:
        dt_to = today
        dt_from = datetime(today.year, today.month, today.day).replace(day=1)
        if dt_from.month == 1:
            dt_from = dt_from.replace(year=dt_from.year - 1, month=12)
        else:
            dt_from = dt_from.replace(month=dt_from.month - 1)

    chunks = generate_monthly_chunks(dt_from, dt_to)

    log("=" * 60)
    log("PHASE 1: METADATA SCRAPE")
    log("=" * 60)
    log(f"Date range: {dt_from.strftime('%Y-%m-%d')} to {dt_to.strftime('%Y-%m-%d')}")
    log(f"Monthly chunks: {len(chunks)}")
    log(f"Max filings: {'unlimited' if max_filings <= 0 else max_filings}")
    log("")

    session = _requests.Session()
    session.headers.update(
        {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    )

    all_filings: list = []
    saved_ids: set[str] = set()
    ingested_tickers: set[str] = set()

    for chunk_idx, (chunk_from, chunk_to) in enumerate(chunks, 1):
        if max_filings > 0 and len(saved_ids) >= max_filings:
            log(f"Reached --limit {max_filings}, stopping.")
            break

        from_str = chunk_from.strftime("%Y%m%d")
        to_str = chunk_to.strftime("%Y%m%d")
        log(
            f"--- CHUNK {chunk_idx}/{len(chunks)}: "
            f"{chunk_from.strftime('%Y-%m-%d')} to {chunk_to.strftime('%Y-%m-%d')} ---"
        )

        remaining = 0
        if max_filings > 0:
            remaining = max_filings - len(saved_ids)

        try:
            chunk_filings = fetch_chunk_via_api(
                session, from_str, to_str, max_records=remaining
            )
        except Exception as e:
            log(f"  ERROR: {e}")
            log("  Skipping chunk, will retry on next run.")
            continue

        chunk_new = 0
        for f in chunk_filings:
            fid = hashlib.md5(
                f"{f['stockCode']}{f['date']}{f.get('title', '')}".encode()
            ).hexdigest()[:16]
            if fid not in saved_ids:
                saved_ids.add(fid)
                all_filings.append(f)
                chunk_new += 1
                raw_code = str(f["stockCode"]).lstrip("0") or "0"
                ingested_tickers.add(f"{raw_code.zfill(4)}.HK")

        log(
            f"  Fetched {len(chunk_filings)} records, {chunk_new} new "
            f"(total unique: {len(saved_ids)})"
        )

    log("")
    log(f"Total unique filings fetched: {len(all_filings)}")

    if not all_filings:
        log("No filings to save.")
        return 0, ingested_tickers

    log("Saving filings to SurrealDB...")
    total_saved = 0
    SAVE_BATCH = 50
    for i in range(0, len(all_filings), SAVE_BATCH):
        batch = all_filings[i : i + SAVE_BATCH]
        count = _save_filings_batch_metadata(batch, dry_run)
        total_saved += count
        if (i + SAVE_BATCH) % 500 == 0 or i + SAVE_BATCH >= len(all_filings):
            log(
                f"  Progress: {min(i + SAVE_BATCH, len(all_filings))}/{len(all_filings)} "
                f"({total_saved} saved)"
            )

    log("")
    log(f"Complete: {total_saved} filings saved to database ({len(saved_ids)} unique IDs)")
    return total_saved, ingested_tickers


# ---------------------------------------------------------------------------
# Phase 2: Document backfill
# ---------------------------------------------------------------------------

def run_phase2(
    batch_size: int = 50,
    max_workers: int = MAX_DOWNLOAD_WORKERS,
    limit: int = 0,
) -> dict:
    """Download and process documents for filings that have metadata but no document content."""
    stats = {
        "total_missing": 0,
        "total_processed": 0,
        "docs_downloaded": 0,
        "texts_extracted": 0,
        "tables_total": 0,
        "skipped": 0,
        "errors": 0,
    }

    log("=" * 60)
    log("PHASE 2: DOCUMENT BACKFILL")
    log("=" * 60)
    log(f"Workers: {max_workers}, Batch size: {batch_size}, Limit: {limit or 'unlimited'}")
    log("")

    count_result = surreal_query(
        "SELECT count() AS cnt FROM exchange_filing "
        "WHERE documentStatus IS NONE "
        "AND documentUrl IS NOT NONE "
        "GROUP ALL;",
        timeout=120,
    )
    total_missing = 0
    if isinstance(count_result, list) and len(count_result) > 0:
        r = count_result[0].get("result", [])
        if r and isinstance(r, list) and len(r) > 0:
            total_missing = r[0].get("cnt", 0)
    stats["total_missing"] = total_missing
    log(f"Filings needing processing: {total_missing}")

    if total_missing == 0:
        log("All filings already processed. Nothing to backfill.")
        return stats

    effective_limit = limit if limit > 0 else total_missing
    batch_num = 0
    consecutive_stalls = 0
    MAX_STALLS = 3

    while stats["total_processed"] < effective_limit:
        batch_num += 1
        remaining = effective_limit - stats["total_processed"]
        this_batch = min(batch_size, remaining)

        fetch_sql = (
            f"SELECT id, filingId, documentUrl, filingDate FROM exchange_filing "
            f"WHERE documentStatus IS NONE "
            f"AND documentUrl IS NOT NONE "
            f"ORDER BY filingDate DESC "
            f"LIMIT {this_batch};"
        )
        result = surreal_query(fetch_sql, timeout=120)
        filings: list = []
        if isinstance(result, list) and len(result) > 0:
            filings = result[0].get("result", [])
        if not filings:
            log(f"No more filings to process (batch {batch_num}).")
            break

        log(f"Batch {batch_num}: Processing {len(filings)} filings...")

        download_tasks: list = []
        for f in filings:
            record_id = str(f.get("id", ""))
            fid = record_id.split(":")[-1] if ":" in record_id else f.get("filingId", "")
            doc_url = f.get("documentUrl", "")
            if fid and doc_url:
                download_tasks.append((fid, doc_url))
            elif fid:
                _mark_filing_status(fid, "skipped", "no_document_url")
                stats["skipped"] += 1

        if not download_tasks:
            stats["total_processed"] += len(filings)
            continue

        # Download in parallel
        downloaded_docs: list = []
        batch_skipped = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_download_worker, t): t for t in download_tasks}
            for future in as_completed(futures):
                try:
                    fid, doc_url, raw_bytes, size_bytes, skip_reason = future.result()
                    if raw_bytes:
                        downloaded_docs.append((fid, doc_url, raw_bytes, size_bytes))
                    else:
                        _mark_filing_status(fid, "skipped", skip_reason or "download_failed")
                        batch_skipped += 1
                except Exception as e:
                    stats["errors"] += 1
                    log(f"  Download error: {e}")

        stats["skipped"] += batch_skipped
        log(f"  Downloaded: {len(downloaded_docs)}, Skipped: {batch_skipped}")

        # Extract text/tables sequentially
        batch_downloaded = 0
        batch_texts = 0
        batch_tables = 0
        for fid, doc_url, raw_bytes, size_bytes in downloaded_docs:
            extracted_text = ""
            tables_json: list = []
            try:
                old_stderr = sys.stderr
                sys.stderr = io.StringIO()
                try:
                    extracted_text, tables_json = extract_content_with_tables(
                        raw_bytes, doc_url
                    )
                finally:
                    sys.stderr = old_stderr
            except Exception as e:
                log(f"  Text extraction error for {fid}: {e}")

            success, error_code = _save_document_to_filing(
                fid, raw_bytes, size_bytes, doc_url, extracted_text, tables_json
            )
            if success:
                batch_downloaded += 1
                if extracted_text:
                    batch_texts += 1
                if tables_json:
                    batch_tables += len(tables_json)
            else:
                _mark_filing_status(fid, "failed", error_code or "save_error")
                stats["errors"] += 1

        stats["docs_downloaded"] += batch_downloaded
        stats["texts_extracted"] += batch_texts
        stats["tables_total"] += batch_tables
        stats["total_processed"] += len(filings)

        log(
            f"  Batch {batch_num}: {batch_downloaded} docs saved, "
            f"{batch_texts} with text, {batch_tables} tables"
        )

        # Stall detection
        batch_status_updates = batch_downloaded + batch_skipped
        if batch_status_updates == 0:
            consecutive_stalls += 1
            log(
                f"  WARNING: No filings updated in this batch "
                f"(stall {consecutive_stalls}/{MAX_STALLS})"
            )
            if consecutive_stalls >= MAX_STALLS:
                log(f"  BREAKING: {MAX_STALLS} consecutive stalls detected. Stopping.")
                break
        else:
            consecutive_stalls = 0

        pct = min(100, (stats["total_processed"] / total_missing) * 100)
        log(f"  Progress: {stats['total_processed']}/{total_missing} ({pct:.1f}%)")

    log("")
    log("=" * 60)
    log("BACKFILL COMPLETE")
    log("=" * 60)
    log(f"Total processed:  {stats['total_processed']}")
    log(f"Docs downloaded:  {stats['docs_downloaded']}")
    log(f"Texts extracted:  {stats['texts_extracted']}")
    log(f"Tables extracted: {stats['tables_total']}")
    log(f"Skipped:          {stats['skipped']}")
    log(f"Errors:           {stats['errors']}")
    return stats
