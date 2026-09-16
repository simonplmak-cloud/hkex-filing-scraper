"""Cross-sink data-fidelity contracts.

Each sink stores the same record families in its own model, so the guarantees that keep the
copies comparable must be explicit and tested: an integrity hash on every sink, exact
identifier comparison, a real dedup version for ClickHouse, uniqueness for Neo4j, timezone-aware
timestamps, and concurrency behaviour for the embedded engines.
"""

from __future__ import annotations

import pathlib
import re

from hkex_scraper import db, db_postgres, pipeline
from hkex_scraper.sinks import clickhouse, dialects, mongodb, neo4j
from hkex_scraper.sinks import duckdb as duckdb_module
from hkex_scraper.sinks import sqlite as sqlite_module


RELATIONAL_DIALECTS = [
    dialects.PostgresDialect(),
    dialects.MySQLDialect("mysql"),
    dialects.MySQLDialect("mariadb"),
    dialects.SQLiteDialect(),
    dialects.DuckDBDialect(),
]


class TestIntegrityHashEverywhere:
    """`document_sha256` must reach every sink, not just the relational ones."""

    def test_relational_column_list_and_ddl(self):
        assert "document_sha256" in dialects.DOCUMENT_COLUMNS
        for dialect in RELATIONAL_DIALECTS:
            ddl = " ".join(dialect.ddl())
            assert "document_sha256" in ddl, f"{dialect.id} DDL is missing document_sha256"

    def test_clickhouse_column_list_and_ddl(self):
        assert "document_sha256" in clickhouse._DOCUMENT_COLUMNS
        assert "document_sha256" in " ".join(clickhouse._DDL)

    def test_mongodb_field_list(self):
        assert "document_sha256" in mongodb._DOCUMENT_FIELDS

    def test_neo4j_upsert_sets_the_property(self):
        source = pathlib.Path(neo4j.__file__).read_text(encoding="utf-8")
        assert "f.documentSha256 = $sha256" in source
        assert '"sha256": payload.get("document_sha256")' in source

    def test_surrealdb_schema_declares_it(self):
        assert "documentSha256" in db._build_schema_sql()

    def test_postgres_schema_and_upsert_include_it(self):
        assert "document_sha256" in " ".join(db_postgres._build_postgres_schema_sql())
        assert "document_sha256" in db_postgres._build_upsert_document_sql()

    def test_payload_carries_both_hashes(self):
        payload = pipeline.document_payload("fid", b"%PDF-1.4 data", 13, "x.pdf", "text", [])
        assert payload["document_sha256"]
        assert len(payload["document_sha256"]) == 64
        assert payload["document_hash"] and len(payload["document_hash"]) == 32

    def test_payload_hashes_are_empty_without_bytes(self):
        payload = pipeline.document_payload("fid", b"", 0, "x.pdf")
        assert payload["document_sha256"] == ""
        assert payload["document_hash"] == ""

    def test_sha256_detects_a_changed_document(self):
        first = pipeline.document_payload("fid", b"a", 1, "x.pdf")["document_sha256"]
        second = pipeline.document_payload("fid", b"b", 1, "x.pdf")["document_sha256"]
        assert first != second


class TestEngineSafeguards:
    """Properties that were assumed before; now asserted."""

    def test_mysql_tables_are_utf8mb4(self):
        for dialect in (dialects.MySQLDialect("mysql"), dialects.MySQLDialect("mariadb")):
            for statement in dialect.ddl():
                assert "utf8mb4" in statement, f"{dialect.id} DDL is not utf8mb4: {statement[:80]}"

    def test_sqlite_enables_wal_and_a_busy_timeout(self):
        source = pathlib.Path(sqlite_module.__file__).read_text(encoding="utf-8")
        assert "PRAGMA journal_mode = WAL" in source
        assert "PRAGMA busy_timeout" in source
        assert "timeout=BUSY_TIMEOUT_SECONDS" in source

    def test_duckdb_retries_transaction_conflicts(self):
        source = pathlib.Path(duckdb_module.__file__).read_text(encoding="utf-8")
        assert "CONFLICT_RETRIES" in source
        assert "_is_conflict" in source

    def test_clickhouse_has_a_dedup_version_and_ordered_by_key(self):
        ddl = " ".join(clickhouse._DDL)
        assert "ReplacingMergeTree(updated_at)" in ddl
        assert "ORDER BY filing_id" in ddl

    def test_clickhouse_reads_use_final(self):
        source = pathlib.Path(clickhouse.__file__).read_text(encoding="utf-8")
        assert " FINAL " in source

    def test_neo4j_constrains_the_filing_id(self):
        constraints = " ".join(neo4j._CONSTRAINTS)
        assert "REQUIRE f.filingId IS UNIQUE" in constraints
        assert "REQUIRE c.id IS UNIQUE" in constraints

    def test_mongodb_keys_on_the_source_id(self):
        source = pathlib.Path(mongodb.__file__).read_text(encoding="utf-8")
        assert '{"_id": filing_id}' in source


class TestTimestampHandling:
    def test_updated_at_is_timezone_aware(self):
        filing = {
            "stockCode": "01461",
            "date": "2026-02-11",
            "title": "Announcement",
            "companyTicker": "1461.HK",
            "exchange": "SEHK",
        }
        record = pipeline._filing_record(filing)
        updated_at = record["updated_at"]
        assert updated_at.tzinfo is not None, "updated_at must be UTC-aware"
        assert updated_at.utcoffset().total_seconds() == 0

    def test_relational_datetime_columns_are_timezone_capable(self):
        assert dialects.PostgresDialect().datetime_type == "timestamptz"
        assert dialects.MySQLDialect().datetime_type == "datetime(6)"
        assert re.fullmatch(r"text", dialects.SQLiteDialect().datetime_type)
