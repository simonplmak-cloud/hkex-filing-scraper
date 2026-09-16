"""Unit tests for explicit sink configuration and connection helpers."""

from __future__ import annotations

from hkex_scraper import config


class TestTargetParsing:
    def test_parse_target_orders_dedupes_and_lowercases(self):
        assert config.parse_target(" MySQL , sqlite ,mysql,") == ["mysql", "sqlite"]

    def test_parse_target_empty(self):
        assert config.parse_target("") == []
        assert config.parse_target("   ") == []

    def test_sink_ids_reads_database_target(self, monkeypatch):
        monkeypatch.setattr(config, "DATABASE_TARGET", "sqlite,postgres")
        assert config.sink_ids() == ["sqlite", "postgres"]

    def test_no_silent_default(self, monkeypatch):
        monkeypatch.setattr(config, "DATABASE_TARGET", "")
        assert config.sink_ids() == []


class TestCliOverride:
    def test_apply_database_target(self, monkeypatch):
        monkeypatch.setattr(config, "DATABASE_TARGET", "postgres")
        result = config.apply_database_target("sqlite,mysql")
        assert result == ["sqlite", "mysql"]
        assert config.sink_ids() == ["sqlite", "mysql"]

    def test_apply_empty_clears(self, monkeypatch):
        monkeypatch.setattr(config, "DATABASE_TARGET", "postgres")
        config.apply_database_target("")
        assert config.sink_ids() == []


class TestMySQLConnection:
    def test_discrete_variables(self, monkeypatch):
        for key in (
            "MYSQL_DSN",
            "MYSQL_HOST",
            "MYSQL_PORT",
            "MYSQL_DATABASE",
            "MYSQL_USER",
            "MYSQL_PASSWORD",
        ):
            monkeypatch.delenv(key, raising=False)
        monkeypatch.setenv("MYSQL_HOST", "db.local")
        monkeypatch.setenv("MYSQL_DATABASE", "hkex")
        monkeypatch.setenv("MYSQL_USER", "simon")
        monkeypatch.setenv("MYSQL_PASSWORD", "secret")
        kwargs = config.mysql_conn_kwargs("MYSQL")
        assert kwargs == {
            "host": "db.local",
            "port": 3306,
            "database": "hkex",
            "user": "simon",
            "password": "secret",
        }

    def test_incomplete_is_empty(self, monkeypatch):
        monkeypatch.delenv("MYSQL_DSN", raising=False)
        monkeypatch.setenv("MYSQL_HOST", "db.local")
        monkeypatch.delenv("MYSQL_DATABASE", raising=False)
        monkeypatch.delenv("MYSQL_USER", raising=False)
        assert config.mysql_conn_kwargs("MYSQL") == {}

    def test_dsn_is_parsed(self, monkeypatch):
        monkeypatch.setenv("MYSQL_DSN", "mysql://user:p%40ss@db.local:3307/hkex")
        kwargs = config.mysql_conn_kwargs("MYSQL")
        assert kwargs["host"] == "db.local"
        assert kwargs["port"] == 3307
        assert kwargs["database"] == "hkex"
        assert kwargs["user"] == "user"
        assert kwargs["password"] == "p@ss"

    def test_mariadb_falls_back_to_mysql_vars(self, monkeypatch):
        for key in ("MARIADB_DSN", "MARIADB_HOST", "MARIADB_DATABASE", "MARIADB_USER"):
            monkeypatch.delenv(key, raising=False)
        monkeypatch.setenv("MYSQL_HOST", "db.local")
        monkeypatch.setenv("MYSQL_DATABASE", "hkex")
        monkeypatch.setenv("MYSQL_USER", "simon")
        kwargs = config.mysql_conn_kwargs("MARIADB")
        assert kwargs["host"] == "db.local" and kwargs["database"] == "hkex"


class TestPostgresConnection:
    def test_conninfo_prefers_dsn(self, monkeypatch):
        monkeypatch.setattr(config, "POSTGRES_DSN", "postgresql://x")
        assert config.postgres_conninfo() == "postgresql://x"

    def test_conninfo_empty_without_details(self, monkeypatch):
        monkeypatch.setattr(config, "POSTGRES_DSN", "")
        monkeypatch.setattr(config, "POSTGRES_DATABASE", "")
        assert config.postgres_conninfo() == ""
