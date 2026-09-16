"""Tests for ``--verify``: cross-sink semantic reconciliation.

Filing *counts* matching is not enough — two sinks can hold the same number of rows and still
disagree about which filings, or about a document's integrity hash. These tests pin the
comparison logic, including the cases that must exit non-zero.
"""

from __future__ import annotations

import pathlib

import pytest

from hkex_scraper import main, sinks
from hkex_scraper.sinks.base import ERR_NONE, Sink, SinkCapabilities, code


class DigestSink(Sink):
    """A sink that returns scripted digests."""

    def __init__(self, sink_id: str, rows, *, err: str = "", available: bool = True):
        super().__init__()
        self._id = sink_id
        self._rows = rows
        self._err = err
        self._available = available

    @property
    def id(self) -> str:  # type: ignore[override]
        return self._id

    @property
    def label(self) -> str:  # type: ignore[override]
        return self._id

    @property
    def capabilities(self) -> SinkCapabilities:  # type: ignore[override]
        return SinkCapabilities(upsert=True, reads=True, edges=False, json=True, arrays=True)

    def available(self) -> bool:
        return self._available

    def unavailable_reason(self) -> str:
        return f"{self._id} sink requires configuration"

    def close(self) -> None:
        return None

    def read_filing_digests(self):
        return list(self._rows), self._err


def _rows(*pairs):
    return [{"filing_id": fid, "document_sha256": sha} for fid, sha in pairs]


class TestVerifyReport:
    def test_identical_sinks_pass(self):
        text, ok = main._format_verify_report(
            {"postgres": _rows(("a", "1"), ("b", "2")), "sqlite": _rows(("a", "1"), ("b", "2"))},
            {},
        )
        assert ok is True
        assert "Verify: OK" in text
        assert "identical to" in text

    def test_missing_ids_fail(self):
        text, ok = main._format_verify_report(
            {"postgres": _rows(("a", "1"), ("b", "2")), "sqlite": _rows(("a", "1"))},
            {},
        )
        assert ok is False
        assert "missing 1 id" in text
        assert "Verify: MISMATCH" in text

    def test_extra_ids_fail(self):
        text, ok = main._format_verify_report(
            {"postgres": _rows(("a", "1")), "sqlite": _rows(("a", "1"), ("b", "2"))},
            {},
        )
        assert ok is False
        assert "extra id" in text

    def test_hash_mismatch_fails_even_when_ids_match(self):
        text, ok = main._format_verify_report(
            {"postgres": _rows(("a", "1")), "sqlite": _rows(("a", "9"))},
            {},
        )
        assert ok is False
        assert "hash mismatch" in text

    def test_empty_hash_on_one_sink_is_not_a_mismatch(self):
        # A document that was never downloaded has no hash anywhere; that is agreement, not drift.
        text, ok = main._format_verify_report(
            {"postgres": _rows(("a", "")), "sqlite": _rows(("a", ""))},
            {},
        )
        assert ok is True

    def test_one_hash_present_and_one_absent_is_not_a_mismatch(self):
        # Only compare when both sides actually have a hash.
        text, ok = main._format_verify_report(
            {"postgres": _rows(("a", "1")), "sqlite": _rows(("a", ""))},
            {},
        )
        assert ok is True

    def test_single_comparable_sink_is_not_applicable(self):
        text, ok = main._format_verify_report({"postgres": _rows(("a", "1"))}, {})
        assert ok is True
        assert "N/A" in text

    def test_unsupported_sinks_are_named_not_ignored(self):
        text, ok = main._format_verify_report(
            {"postgres": _rows(("a", "1")), "sqlite": _rows(("a", "1"))},
            {"mongodb": "cannot enumerate filings"},
        )
        assert ok is True
        assert "MongoDB: not comparable" in text

    def test_three_sinks_compare_against_the_first(self):
        text, ok = main._format_verify_report(
            {
                "postgres": _rows(("a", "1")),
                "sqlite": _rows(("a", "1")),
                "mysql": _rows(("a", "2")),
            },
            {},
        )
        assert ok is False
        assert "MySQL" in text and "hash mismatch" in text


class TestVerifyCommand:
    def _install(self, monkeypatch, *digest_sinks):
        monkeypatch.setattr(sinks, "enabled_sinks", lambda: list(digest_sinks))

    def test_exit_zero_when_sinks_agree(self, monkeypatch, capsys):
        self._install(
            monkeypatch,
            DigestSink("postgres", _rows(("a", "1"))),
            DigestSink("sqlite", _rows(("a", "1"))),
        )
        with pytest.raises(SystemExit) as excinfo:
            main._verify_report()
        assert excinfo.value.code == 0
        assert "Verify: OK" in capsys.readouterr().out

    def test_exit_non_zero_when_sinks_disagree(self, monkeypatch, capsys):
        self._install(
            monkeypatch,
            DigestSink("postgres", _rows(("a", "1"))),
            DigestSink("sqlite", _rows(("a", "1"), ("b", "2"))),
        )
        with pytest.raises(SystemExit) as excinfo:
            main._verify_report()
        assert excinfo.value.code == 1
        assert "MISMATCH" in capsys.readouterr().out

    def test_requires_two_sinks(self, monkeypatch, capsys):
        self._install(monkeypatch, DigestSink("postgres", _rows(("a", "1"))))
        with pytest.raises(SystemExit) as excinfo:
            main._verify_report()
        assert excinfo.value.code == 1
        assert "two or more configured sinks" in capsys.readouterr().out

    def test_unsupported_capability_is_skipped_not_fatal(self, monkeypatch, capsys):
        self._install(
            monkeypatch,
            DigestSink("postgres", _rows(("a", "1"))),
            DigestSink("sqlite", _rows(("a", "1"))),
            DigestSink("mongodb", [], err=code("mongodb", "UNSUPPORTED")),
        )
        with pytest.raises(SystemExit) as excinfo:
            main._verify_report()
        assert excinfo.value.code == 0
        assert "not comparable" in capsys.readouterr().out

    def test_a_read_error_is_fatal(self, monkeypatch, capsys):
        self._install(
            monkeypatch,
            DigestSink("postgres", _rows(("a", "1"))),
            DigestSink("sqlite", [], err=code("sqlite", "CONNECT_FAILED")),
        )
        with pytest.raises(SystemExit) as excinfo:
            main._verify_report()
        assert excinfo.value.code == 1
        assert "could not read filings" in capsys.readouterr().out

    def test_unavailable_sink_is_fatal(self, monkeypatch, capsys):
        self._install(
            monkeypatch,
            DigestSink("postgres", _rows(("a", "1"))),
            DigestSink("sqlite", [], available=False),
        )
        with pytest.raises(SystemExit) as excinfo:
            main._verify_report()
        assert excinfo.value.code == 1
        assert "requires configuration" in capsys.readouterr().out

    def test_flag_is_registered(self):
        args = main._build_parser().parse_args(["--verify"])
        assert args.verify is True


class TestUnsupportedDefault:
    def test_base_contract_reports_unsupported_rather_than_empty(self):
        rows, err = Sink.read_filing_digests(object.__new__(Sink))
        assert rows == []
        assert err.endswith("UNSUPPORTED")
        assert err != ERR_NONE


class TestDigestQueries:
    """Each adapter must expose an id + integrity hash, using its own idiom."""

    def test_relational_dialects_select_both_columns(self):
        from hkex_scraper.sinks import dialects

        dialects_to_check = (
            dialects.PostgresDialect(),
            dialects.MySQLDialect("mysql"),
            dialects.MySQLDialect("mariadb"),
            dialects.SQLiteDialect(),
            dialects.DuckDBDialect(),
        )
        for dialect in dialects_to_check:
            sql = dialect.select_digests_sql()
            assert "filing_id" in sql and "document_sha256" in sql, dialect.id
            assert "ORDER BY" in sql, f"{dialect.id} must order for a stable diff"

    def test_postgres_upsert_paths_stay_separate_from_reads(self):
        from hkex_scraper import db_postgres

        assert "document_sha256" in db_postgres._build_upsert_document_sql()
        source = pathlib.Path(db_postgres.__file__).read_text(encoding="utf-8")
        assert "SELECT filing_id, document_sha256 FROM exchange_filing ORDER BY filing_id" in source

    def test_clickhouse_reads_final_state(self):
        from hkex_scraper.sinks import clickhouse

        source = pathlib.Path(clickhouse.__file__).read_text(encoding="utf-8")
        assert "SELECT filing_id, document_sha256 FROM" in source
        assert "FINAL ORDER BY filing_id" in source

    def test_mongodb_projects_id_and_hash(self):
        from hkex_scraper.sinks import mongodb

        source = pathlib.Path(mongodb.__file__).read_text(encoding="utf-8")
        assert '{"_id": 1, "document_sha256": 1}' in source

    def test_neo4j_returns_ordered_properties(self):
        from hkex_scraper.sinks import neo4j

        source = pathlib.Path(neo4j.__file__).read_text(encoding="utf-8")
        assert "f.filingId AS filing_id" in source
        assert "f.documentSha256 AS sha" in source

    def test_surrealdb_selects_the_camelcase_fields(self):
        from hkex_scraper.sinks import surrealdb

        source = pathlib.Path(surrealdb.__file__).read_text(encoding="utf-8")
        assert "SELECT filingId, documentSha256 FROM exchange_filing;" in source
