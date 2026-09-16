"""Unit tests for the sink registry and lazy driver loading."""

from __future__ import annotations

import sys

import pytest

from hkex_scraper import config, sinks


class TestRegistry:
    def test_known_ids(self):
        ids = sinks.known_ids()
        for expected in ("postgres", "mysql", "mariadb", "sqlite", "surrealdb"):
            assert expected in ids
        assert ids == sorted(ids)

    def test_get_sink_is_lazy_for_optional_drivers(self, monkeypatch):
        # Selecting the stdlib-backed sink must not import an optional driver.
        for name in list(sys.modules):
            if name.startswith("pymysql"):
                monkeypatch.delitem(sys.modules, name)
        sink = sinks.get_sink("sqlite")
        assert sink.id == "sqlite"
        assert not any(name.startswith("pymysql") for name in sys.modules)

    def test_unknown_id_raises_with_valid_ids(self):
        with pytest.raises(sinks.UnknownSinkError) as excinfo:
            sinks.get_sink("nope")
        message = str(excinfo.value)
        assert "nope" in message
        assert "postgres" in message and "sqlite" in message

    def test_spec_metadata(self):
        assert sinks.spec("postgres").source_available is True
        assert sinks.spec("surrealdb").source_available is False
        assert sinks.spec("mysql").extra == "mysql"


class TestReadRouting:
    def test_read_source_is_first_readable(self, monkeypatch):
        monkeypatch.setattr(config, "DATABASE_TARGET", "sqlite,mysql")
        sinks.reset()
        reader = sinks.read_sink()
        assert reader is not None and reader.id == "sqlite"
        sinks.reset()

    def test_read_source_none_when_no_readable_sink(self, monkeypatch):
        monkeypatch.setattr(config, "DATABASE_TARGET", "")
        sinks.reset()
        assert sinks.read_sink() is None
        sinks.reset()


class TestCapabilities:
    def test_sqlite_declares_capabilities(self):
        sink = sinks.get_sink("sqlite")
        caps = sink.capabilities
        assert caps.model == "relational"
        assert caps.native_upsert is True
        assert caps.reads is True
        assert caps.edges is True

    def test_surrealdb_is_a_graph_sink(self):
        sink = sinks.get_sink("surrealdb")
        assert sink.capabilities.model == "graph"
        assert sink.capabilities.edges is True
