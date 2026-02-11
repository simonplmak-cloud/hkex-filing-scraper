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
# Derivative issuer detection
# ---------------------------------------------------------------------------
_DERIVATIVE_PATTERNS = [
    "DAILY TRADING SUMMARY",
    "DAILY TRADING REPORT",
    "DAILY REPORT ON",
]

# Known derivative issuers: map title keywords to short identifiers
_KNOWN_ISSUERS = {
    "HUATAI": "HUATAI",
    "CITIGROUP": "CITI",
    "UBS": "UBS",
    "HSBC": "HSBC",
    "HK AND SHANGHAI BANK": "HSBC",
    "BOCI": "BOCI",
    "SOCIETE GENERALE": "SOCGEN",
    "BANK OF EAST ASIA": "BEA",
    "GUOTAI JUNAN": "GUOTAI",
    "MORGAN STANLEY": "MORGAN",
    "KOREA INVESTMENT": "KOREA",
    "CITIC": "CITIC",
    "JP MORGAN": "JPM",
    "JPMORGAN": "JPM",
    "GOLDMAN SACHS": "GS",
    "CREDIT SUISSE": "CS",
    "BARCLAYS": "BARCLAYS",
    "BNP PARIBAS": "BNP",
    "MACQUARIE": "MACQUARIE",
    "HAITONG": "HAITONG",
    "STANDARD CHARTERED": "STANCHART",
    "DEUTSCHE BANK": "DB",
    "BOCOM": "BOCOM",
    "CHINA INTERNATIONAL": "CICC",
}


def is_derivative_issuer_filing(title: str) -> bool:
    """Return True if the title indicates a derivative issuer filing (warrants/CBBC)."""
    t = (title or "").upper()
    return any(p in t for p in _DERIVATIVE_PATTERNS)


def extract_issuer_name(title: str) -> str:
    """Extract the issuer short name from a derivative filing title.

    Uses a known-issuers lookup first, then falls back to first-word extraction.
    """
    t_upper = (title or "").upper()
    # Check known issuers first (longest match wins)
    for keyword, short in sorted(_KNOWN_ISSUERS.items(), key=lambda x: -len(x[0])):
        if keyword in t_upper:
            return short

    # Fallback: extract the first word before the 'Daily' separator
    for sep in [" - Daily", " -Daily", " Daily"]:
        idx = title.find(sep)
        if idx == -1:
            idx = t_upper.find(sep.upper())
        if idx > 0:
            issuer = title[:idx].strip()
            first_word = issuer.split()[0].upper() if issuer else ""
            first_word = re.sub(r"[^A-Z0-9]", "", first_word)
            return first_word if first_word else "DERIV"
    return "DERIV"


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
