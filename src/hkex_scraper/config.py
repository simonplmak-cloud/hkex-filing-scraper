"""Centralised configuration loaded from environment variables / .env file."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote

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
# Database sink selection
# ---------------------------------------------------------------------------
# DATABASE_TARGET selects which persistence sink(s) receive scraped records:
#   "surrealdb" (default) -> SurrealDB only (backward compatible)
#   "postgres" / "postgresql" / "pg" -> PostgreSQL only
#   "both" / "dual" -> SurrealDB and PostgreSQL
_SINK_ALIASES_BOTH = {"both", "dual", "surrealdb,postgres", "postgres,surrealdb"}
_SINK_ALIASES_POSTGRES = {"postgres", "postgresql", "pg"}


def _resolve_sinks(target: str) -> tuple[bool, bool]:
    """Map a ``DATABASE_TARGET`` value to ``(surrealdb_enabled, postgres_enabled)``."""
    value = (target or "").strip().lower()
    if value in _SINK_ALIASES_BOTH:
        return True, True
    if value in _SINK_ALIASES_POSTGRES:
        return False, True
    # Default / "surrealdb" / any unrecognised value keeps the original behaviour.
    return True, False


DATABASE_TARGET: str = os.environ.get("DATABASE_TARGET", "surrealdb")
SURREALDB_ENABLED: bool
POSTGRES_ENABLED: bool
SURREALDB_ENABLED, POSTGRES_ENABLED = _resolve_sinks(DATABASE_TARGET)

# ---------------------------------------------------------------------------
# PostgreSQL connection (optional sink)
# ---------------------------------------------------------------------------
POSTGRES_DSN: str = os.environ.get("POSTGRES_DSN", "")
POSTGRES_HOST: str = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT: str = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DATABASE: str = os.environ.get("POSTGRES_DATABASE", "")
POSTGRES_USER: str = os.environ.get("POSTGRES_USER", "")
POSTGRES_PASSWORD: str = os.environ.get("POSTGRES_PASSWORD", "")
POSTGRES_SCHEMA: str = os.environ.get("POSTGRES_SCHEMA", "public")
POSTGRES_MIN_POOL: int = int(os.environ.get("POSTGRES_MIN_POOL", "1"))
POSTGRES_MAX_POOL: int = int(
    os.environ.get("POSTGRES_MAX_POOL", os.environ.get("MAX_DOWNLOAD_WORKERS", "15"))
)


def postgres_conninfo() -> str:
    """Return the PostgreSQL connection string, or ``""`` when not configured.

    Prefers ``POSTGRES_DSN``; otherwise assembles a libpq keyword string from the
    discrete ``POSTGRES_*`` variables. Never logs or returns credentials to callers
    other than the driver.
    """
    if POSTGRES_DSN:
        return POSTGRES_DSN
    if POSTGRES_DATABASE and POSTGRES_USER:
        user = quote(POSTGRES_USER, safe="")
        password = quote(POSTGRES_PASSWORD, safe="")
        database = quote(POSTGRES_DATABASE, safe="")
        return f"postgresql://{user}:{password}@{POSTGRES_HOST}:{POSTGRES_PORT}/{database}"
    return ""


def apply_database_target(target: str) -> tuple[bool, bool]:
    """Re-resolve the sink selection at runtime (e.g. from a CLI override).

    Updates the module-level flags in place so callers reading
    ``config.postgres_enabled()`` observe the override for the rest of the run.
    """
    global DATABASE_TARGET, SURREALDB_ENABLED, POSTGRES_ENABLED
    DATABASE_TARGET = target or "surrealdb"
    SURREALDB_ENABLED, POSTGRES_ENABLED = _resolve_sinks(DATABASE_TARGET)
    return SURREALDB_ENABLED, POSTGRES_ENABLED


def surrealdb_enabled() -> bool:
    """Whether the SurrealDB sink is active for this run."""
    return SURREALDB_ENABLED


def postgres_enabled() -> bool:
    """Whether the PostgreSQL sink is active for this run."""
    return POSTGRES_ENABLED


def postgres_required() -> bool:
    """True when PostgreSQL is the only enabled sink and therefore cannot degrade."""
    return POSTGRES_ENABLED and not SURREALDB_ENABLED


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
MAX_SQL_BODY_SIZE: int = 900 * 1024  # ~900 KB text limit (SurrealDB /sql endpoint = 1 MiB)
MAX_RPC_BODY_SIZE: int = 3_800_000  # ~3.8 MB safe limit (SurrealDB /rpc endpoint = 4 MiB)

# ---------------------------------------------------------------------------
# HKEx API
# ---------------------------------------------------------------------------
HKEX_BASE_URL: str = "https://www1.hkexnews.hk"
HKEX_SEARCH_PAGE: str = f"{HKEX_BASE_URL}/search/titlesearch.xhtml"
HKEX_API_ENDPOINT: str = f"{HKEX_BASE_URL}/search/titleSearchServlet.do"
