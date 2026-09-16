"""Unit tests for the shared relational sink: degradation and redaction."""

from __future__ import annotations

from hkex_scraper import sinks
from hkex_scraper.sinks import mysql as mysql_module
from hkex_scraper.sinks import sqlite as sqlite_module


class TestDegradation:
    def test_sqlite_unconfigured_degrades(self, monkeypatch):
        monkeypatch.setattr(sqlite_module.config, "SQLITE_PATH", "")
        sink = sqlite_module.SQLiteSink()
        assert sink.available() is False
        assert "SQLITE_PATH" in sink.unavailable_reason()
        assert sink.upsert_filings([{"filing_id": "x"}]) == (0, "SQLITE_DSN_MISSING")
        ok, code = sink.ensure_schema()
        assert ok is False and code == "SQLITE_DSN_MISSING"
        # No exception raised anywhere above.

    def test_mysql_driver_missing_degrades(self, monkeypatch):
        monkeypatch.setattr(mysql_module, "_PYMYSQL_AVAILABLE", False)
        sink = mysql_module.MySQLSink()
        assert sink.available() is False
        assert "PyMySQL" in sink.unavailable_reason()
        assert sink.upsert_filings([{"filing_id": "x"}]) == (0, "MYSQL_DRIVER_MISSING")
        ok, code = sink.upsert_document("x", {})
        assert ok is False and code == "MYSQL_DRIVER_MISSING"

    def test_mysql_unconfigured_reports_setting(self, monkeypatch):
        monkeypatch.setattr(mysql_module, "_PYMYSQL_AVAILABLE", True)
        monkeypatch.setattr(mysql_module.config, "mysql_conn_kwargs", lambda *_a, **_k: {})
        sink = mysql_module.MySQLSink()
        assert sink.available() is False
        assert "MYSQL_DSN" in sink.unavailable_reason()

    def test_unknown_edge_kind_is_payload_error(self, monkeypatch):
        monkeypatch.setattr(sqlite_module.config, "SQLITE_PATH", ":memory:")
        sink = sqlite_module.SQLiteSink()
        created, code = sink.upsert_edges(
            [{"company_ticker": "0451.HK", "filing_id": "f"}], "bogus"
        )
        assert created == 0 and code == "SQLITE_PAYLOAD_ERROR"


class TestCredentialRedaction:
    def test_dsn_password_redacted(self):
        redacted = sinks.redact("connection failed: host=db password=supersecret123 user=simon")
        assert "supersecret123" not in redacted
        assert "***" in redacted

    def test_url_credentials_redacted(self):
        redacted = sinks.redact("could not connect to mysql://simon:topsecret@db.local:3306/hkex")
        assert "topsecret" not in redacted
        assert "***" in redacted


class TestCapabilityDeclaration:
    def test_relational_capabilities(self):
        caps = sinks.get_sink("sqlite").capabilities
        assert caps.model == "relational"
        assert caps.native_upsert and caps.reads and caps.edges
