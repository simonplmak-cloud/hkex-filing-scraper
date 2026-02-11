"""Shared utilities: logging, string helpers, filing classification."""

from __future__ import annotations

import os
import re
from datetime import datetime
from typing import List, Tuple

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
_LOG_FILE = None


def set_log_file(fh) -> None:
    """Set the global log file handle."""
    global _LOG_FILE
    _LOG_FILE = fh


def close_log_file() -> None:
    """Close the global log file handle."""
    global _LOG_FILE
    if _LOG_FILE:
        try:
            _LOG_FILE.close()
        except Exception:
            pass
        _LOG_FILE = None


def log(msg: str) -> None:
    """Print a timestamped message to stdout and the log file (if open)."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line, flush=True)
    if _LOG_FILE:
        _LOG_FILE.write(line + "\n")
        _LOG_FILE.flush()


# ---------------------------------------------------------------------------
# String helpers
# ---------------------------------------------------------------------------
def squash_ws(s: str) -> str:
    """Remove control / zero-width chars; collapse whitespace."""
    cleaned = "".join(ch if (ch.isprintable() or ch == " ") else " " for ch in str(s))
    return re.sub(r"\s+", " ", cleaned).strip()


def escape_sql(s) -> str:
    """Escape a value for embedding in a SurrealQL single-quoted string literal."""
    if s is None:
        return ""
    result = str(s)
    result = "".join(c if c.isprintable() or c == " " else "" for c in result)
    result = result.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    result = result.replace("\\", "\\\\")
    result = result.replace("'", "\\'")
    return result


# ---------------------------------------------------------------------------
# Filing classification
# ---------------------------------------------------------------------------
def classify_filing(title: str) -> Tuple[str, str]:
    """Classify a filing by its title into (type_code, subtype_label)."""
    t = (title or "").upper()
    if "ANNUAL REPORT" in t:
        return ("ANNUAL_REPORT", "Annual Report")
    if "ANNUAL RESULTS" in t:
        return ("RESULTS", "Annual Results")
    if "INTERIM REPORT" in t:
        return ("RESULTS", "Interim Report")
    if "INTERIM RESULTS" in t:
        return ("RESULTS", "Interim Results")
    if "QUARTERLY" in t:
        return ("RESULTS", "Quarterly")
    if "DIVIDEND" in t:
        return ("DIVIDEND", "Dividend")
    if "TRANSACTION" in t or "ACQUISITION" in t:
        return ("TRANSACTION", "Transaction")
    if "DIRECTOR" in t:
        return ("DIRECTOR", "Director")
    if "CIRCULAR" in t:
        return ("CIRCULAR", "Circular")
    if "MEETING" in t or "AGM" in t:
        return ("MEETING", "Meeting")
    return ("OTHER", "Announcement")


# ---------------------------------------------------------------------------
# Cross-reference extraction
# ---------------------------------------------------------------------------
def extract_referenced_tickers(title: str, own_stock_code: str) -> List[str]:
    """Extract stock codes referenced in a filing title, excluding the filing's own code."""
    referenced: set[str] = set()
    own_normalized = own_stock_code.lstrip("0") or "0"

    for m in re.finditer(
        r"stock\s*code[s]?[:\s]+((?:\d{3,5}[,\s]*)+)", title, re.IGNORECASE
    ):
        for code in re.findall(r"\d{3,5}", m.group(1)):
            referenced.add(code)

    for m in re.finditer(r"\((\d{3,5})\.HK\)", title, re.IGNORECASE):
        referenced.add(m.group(1))

    result: List[str] = []
    for code in referenced:
        normalized = code.lstrip("0") or "0"
        if normalized != own_normalized:
            result.append(f"{normalized.zfill(4)}.HK")
    return sorted(set(result))
