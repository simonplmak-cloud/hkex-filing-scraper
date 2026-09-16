"""MySQL / MariaDB sink — optional ``PyMySQL`` driver.

PyMySQL is a pure-Python DB-API driver, so no C toolchain is required. Both
engines share :class:`MySQLDialect`; only the connection settings differ.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .. import config
from .base import redact
from .dialects import MySQLDialect
from .relational import RelationalDriver, RelationalSink

try:  # pragma: no cover - exercised via monkeypatching in tests
    import pymysql  # type: ignore
    from pymysql.cursors import DictCursor  # type: ignore

    _PYMYSQL_AVAILABLE = True
except Exception:  # pragma: no cover - depends on environment
    pymysql = None  # type: ignore
    DictCursor = None  # type: ignore
    _PYMYSQL_AVAILABLE = False


class _MySQLDriver(RelationalDriver):
    def __init__(self, kwargs: Dict[str, Any]) -> None:
        self._kwargs = kwargs
        self._conn = None

    def connect(self) -> None:
        if self._conn is not None:
            return
        if not _PYMYSQL_AVAILABLE or pymysql is None:
            raise RuntimeError("pymysql is not installed")
        self._conn = pymysql.connect(
            **self._kwargs,
            charset="utf8mb4",
            autocommit=False,
            cursorclass=DictCursor,
        )

    def execute(
        self, sql: str, params: Optional[Any] = None, many: bool = False
    ) -> Tuple[bool, str, int]:
        assert self._conn is not None
        try:
            with self._conn.cursor() as cur:
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
            self._conn.commit()
            return True, "", rowcount
        except Exception as exc:  # noqa: BLE001 - surfaced as a code
            try:
                self._conn.rollback()
            except Exception:  # pragma: no cover - best effort
                pass
            return False, redact(str(exc)), 0

    def fetch_all(self, sql: str, params: Optional[Any] = None) -> Tuple[List[Dict[str, Any]], str]:
        assert self._conn is not None
        try:
            with self._conn.cursor() as cur:
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
            with self._conn.cursor() as cur:
                if params is not None:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)
                row = cur.fetchone()
                if row is None:
                    return 0, ""
                if isinstance(row, dict):
                    return next(iter(row.values())), ""
                return row[0], ""
        except Exception as exc:  # noqa: BLE001
            return None, redact(str(exc))

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # pragma: no cover - best effort
                pass
            self._conn = None


class MySQLSink(RelationalSink):
    """Persist records to MySQL."""

    _prefix = "MYSQL"

    def __init__(self) -> None:
        super().__init__(
            "mysql",
            MySQLDialect("mysql"),
            _MySQLDriver(config.mysql_conn_kwargs(self._prefix)),
        )

    def driver_installed(self) -> bool:
        return _PYMYSQL_AVAILABLE

    def configured(self) -> bool:
        return bool(config.mysql_conn_kwargs(self._prefix))

    def unavailable_reason(self) -> str:
        if not _PYMYSQL_AVAILABLE:
            return 'MySQL sink requires PyMySQL (install with: pip install ".[mysql]")'
        if not config.mysql_conn_kwargs(self._prefix):
            return "MySQL sink requires MYSQL_HOST/MYSQL_DATABASE/MYSQL_USER (or MYSQL_DSN)"
        return "MySQL sink is unavailable"


class MariaDBSink(RelationalSink):
    """Persist records to MariaDB (reads ``MARIADB_*``, falling back to ``MYSQL_*``)."""

    _prefix = "MARIADB"

    def __init__(self) -> None:
        super().__init__(
            "mariadb",
            MySQLDialect("mariadb"),
            _MySQLDriver(config.mysql_conn_kwargs(self._prefix)),
        )

    def driver_installed(self) -> bool:
        return _PYMYSQL_AVAILABLE

    def configured(self) -> bool:
        return bool(config.mysql_conn_kwargs(self._prefix))

    def unavailable_reason(self) -> str:
        if not _PYMYSQL_AVAILABLE:
            return 'MariaDB sink requires PyMySQL (install with: pip install ".[mysql]")'
        if not config.mysql_conn_kwargs(self._prefix):
            return (
                "MariaDB sink requires MARIADB_HOST/MARIADB_DATABASE/MARIADB_USER (or MARIADB_DSN)"
            )
        return "MariaDB sink is unavailable"
