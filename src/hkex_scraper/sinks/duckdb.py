"""DuckDB sink — optional ``duckdb`` driver, in-process analytical SQL.

DuckDB speaks a PostgreSQL-like dialect with ``?`` placeholders and
``ON CONFLICT`` upserts, so it reuses the shared relational engine. It has no
``rowcount``, so the dialect appends ``RETURNING 1`` to the statements whose
affected-row count matters.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .. import config
from .base import redact
from .dialects import DuckDBDialect
from .relational import RelationalDriver, RelationalSink

try:  # pragma: no cover - exercised via monkeypatching in tests
    import duckdb  # type: ignore

    _DUCKDB_AVAILABLE = True
except Exception:  # pragma: no cover - depends on environment
    duckdb = None  # type: ignore
    _DUCKDB_AVAILABLE = False


class _DuckDBDriver(RelationalDriver):
    def __init__(self, path: str) -> None:
        self._path = path
        self._conn = None

    def connect(self) -> None:
        if self._conn is not None:
            return
        if not _DUCKDB_AVAILABLE or duckdb is None:
            raise RuntimeError("duckdb is not installed")
        self._conn = duckdb.connect(self._path)

    def execute(
        self, sql: str, params: Optional[Any] = None, many: bool = False
    ) -> Tuple[bool, str, int]:
        assert self._conn is not None
        try:
            if many:
                rows = list(params or [])
                self._conn.executemany(sql, rows)
                return True, "", len(rows)
            cur = self._conn.execute(sql, params) if params is not None else self._conn.execute(sql)
            if cur.description is not None:
                # ``RETURNING`` / SELECT: the number of rows is the affected count.
                return True, "", len(cur.fetchall())
            return True, "", 1
        except Exception as exc:  # noqa: BLE001 - surfaced as a code
            return False, redact(str(exc)), 0

    def fetch_all(self, sql: str, params: Optional[Any] = None) -> Tuple[List[Dict[str, Any]], str]:
        assert self._conn is not None
        try:
            cur = self._conn.execute(sql, params) if params is not None else self._conn.execute(sql)
            names = [d[0] for d in (cur.description or [])]
            return [dict(zip(names, row)) for row in cur.fetchall()], ""
        except Exception as exc:  # noqa: BLE001
            return [], redact(str(exc))

    def fetch_scalar(self, sql: str, params: Optional[Any] = None) -> Tuple[Any, str]:
        assert self._conn is not None
        try:
            cur = self._conn.execute(sql, params) if params is not None else self._conn.execute(sql)
            row = cur.fetchone()
            return (row[0] if row is not None else 0), ""
        except Exception as exc:  # noqa: BLE001
            return None, redact(str(exc))

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # pragma: no cover - best effort
                pass
            self._conn = None


class DuckDBSink(RelationalSink):
    """Persist records to a DuckDB database file (or ``:memory:``)."""

    def __init__(self) -> None:
        super().__init__("duckdb", DuckDBDialect(), _DuckDBDriver(config.DUCKDB_PATH))

    def driver_installed(self) -> bool:
        return _DUCKDB_AVAILABLE

    def configured(self) -> bool:
        return bool(config.DUCKDB_PATH)

    def unavailable_reason(self) -> str:
        if not _DUCKDB_AVAILABLE:
            return 'DuckDB sink requires the duckdb driver (install with: pip install ".[duckdb]")'
        if not config.DUCKDB_PATH:
            return "DuckDB sink requires DUCKDB_PATH (set DUCKDB_PATH=hkex.duckdb or :memory:)"
        return "DuckDB sink is unavailable"
