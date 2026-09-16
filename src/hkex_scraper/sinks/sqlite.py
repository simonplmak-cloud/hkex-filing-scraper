"""SQLite sink — stdlib ``sqlite3``, no optional dependency.

Datetimes are stored as ISO-8601 text and JSON columns as JSON text (see
:class:`SQLiteDialect`).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .. import config
from .base import redact
from .dialects import SQLiteDialect
from .relational import RelationalDriver, RelationalSink


class _SQLiteDriver(RelationalDriver):
    def __init__(self, path: str) -> None:
        self._path = path
        self._conn = None

    def connect(self) -> None:
        if self._conn is not None:
            return
        import sqlite3

        conn = sqlite3.connect(self._path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        self._conn = conn

    def execute(
        self, sql: str, params: Optional[Any] = None, many: bool = False
    ) -> Tuple[bool, str, int]:
        assert self._conn is not None
        try:
            with self._conn:
                cur = self._conn.cursor()
                if many:
                    rows = list(params or [])
                    cur.executemany(sql, rows)
                    rowcount = len(rows)
                elif params is not None:
                    cur.execute(sql, params)
                    rowcount = cur.rowcount if (cur.rowcount or -1) > 0 else 0
                else:
                    cur.execute(sql)
                    rowcount = 0
            return True, "", rowcount
        except Exception as exc:  # noqa: BLE001 - surfaced as a code
            return False, redact(str(exc)), 0

    def fetch_all(self, sql: str, params: Optional[Any] = None) -> Tuple[List[Dict[str, Any]], str]:
        assert self._conn is not None
        try:
            cur = self._conn.cursor()
            if params is not None:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            return [dict(row) for row in cur.fetchall()], ""
        except Exception as exc:  # noqa: BLE001
            return [], redact(str(exc))

    def fetch_scalar(self, sql: str, params: Optional[Any] = None) -> Tuple[Any, str]:
        assert self._conn is not None
        try:
            cur = self._conn.cursor()
            if params is not None:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
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


class SQLiteSink(RelationalSink):
    """Persist records to a SQLite database file."""

    def __init__(self) -> None:
        super().__init__("sqlite", SQLiteDialect(), _SQLiteDriver(config.SQLITE_PATH))

    def driver_installed(self) -> bool:
        return True

    def configured(self) -> bool:
        return bool(config.SQLITE_PATH)

    def unavailable_reason(self) -> str:
        if not config.SQLITE_PATH:
            return "SQLite sink requires SQLITE_PATH (set SQLITE_PATH=hkex.db)"
        return "SQLite sink is unavailable"
