"""Centralised configuration loaded from environment variables / .env file."""

from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Optional dotenv support (graceful if missing)
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:

    def load_dotenv(*_args, **_kwargs):  # type: ignore[misc]
        return False


# Load .env from the current working directory (if present)
_env_path = Path.cwd() / ".env"
if _env_path.exists():
    load_dotenv(_env_path)

# ---------------------------------------------------------------------------
# SurrealDB connection
# ---------------------------------------------------------------------------
SURREAL_ENDPOINT: str = os.environ.get("SURREAL_ENDPOINT", "")
SURREAL_NS: str = os.environ.get("SURREAL_NAMESPACE", "default")
SURREAL_DB: str = os.environ.get("SURREAL_DATABASE", "default")
SURREAL_USER: str = os.environ.get("SURREAL_USERNAME", "root")
SURREAL_PASS: str = os.environ.get("SURREAL_PASSWORD", "")

# ---------------------------------------------------------------------------
# Graph linking (optional)
# ---------------------------------------------------------------------------
# Set COMPANY_TABLE to the name of your company table to enable graph edges.
# Leave empty to disable graph linking entirely.
COMPANY_TABLE: str = os.environ.get("COMPANY_TABLE", "")

# Pattern for converting a ticker like "0451.HK" into a record ID.
# Placeholders: {code} = leading-zero-stripped stock code, {exchange} = exchange suffix.
# Example: "{code}_{exchange}" produces "451_HK".
COMPANY_ID_PATTERN: str = os.environ.get("COMPANY_ID_PATTERN", "{code}_{exchange}")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
LOG_DIR: Path = Path.cwd() / "logs"

# ---------------------------------------------------------------------------
# Tuning
# ---------------------------------------------------------------------------
BATCH_SIZE: int = 100
MAX_DOWNLOAD_WORKERS: int = int(os.environ.get("MAX_DOWNLOAD_WORKERS", "15"))
MAX_DOWNLOAD_SIZE: int = 25 * 1024 * 1024  # 25 MB

# ---------------------------------------------------------------------------
# HKEx API
# ---------------------------------------------------------------------------
HKEX_BASE_URL: str = "https://www1.hkexnews.hk"
HKEX_SEARCH_PAGE: str = f"{HKEX_BASE_URL}/search/titlesearch.xhtml"
HKEX_API_ENDPOINT: str = f"{HKEX_BASE_URL}/search/titleSearchServlet.do"
