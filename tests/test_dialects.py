"""Unit tests for the SQL dialect descriptors (no database required)."""

from __future__ import annotations

from hkex_scraper.sinks.dialects import (
    COVERAGE_COLUMNS,
    DOCUMENT_COLUMNS,
    FILING_COLUMNS,
    MySQLDialect,
    PostgresDialect,
    SQLiteDialect,
)

MYSQL = MySQLDialect()
MARIADB = MySQLDialect("mariadb")
SQLITE = SQLiteDialect()
POSTGRES = PostgresDialect()


class TestUpsertFilings:
    def test_sqlite_uses_qmark_and_excluded(self):
        sql = SQLITE.upsert_filings_sql()
        assert 'INSERT INTO "exchange_filing"' in sql
        assert 'ON CONFLICT ("filing_id") DO UPDATE SET' in sql
        assert "EXCLUDED" in sql
        assert sql.count("?") == len(FILING_COLUMNS)

    def test_sqlite_never_touches_document_columns(self):
        sql = SQLITE.upsert_filings_sql()
        for column in DOCUMENT_COLUMNS:
            assert column not in sql
        assert "filing_id" in sql

    def test_mysql_uses_duplicate_key_and_backticks(self):
        sql = MYSQL.upsert_filings_sql()
        assert "INSERT INTO `exchange_filing`" in sql
        assert "ON DUPLICATE KEY UPDATE" in sql
        assert "VALUES(" in sql
        assert sql.count("%s") == len(FILING_COLUMNS)

    def test_mariadb_quotes_for_its_own_identifier(self):
        sql = MARIADB.upsert_filings_sql()
        assert "ON DUPLICATE KEY UPDATE" in sql
        assert MARIADB.id == "mariadb"

    def test_postgres_uses_excluded(self):
        sql = POSTGRES.upsert_filings_sql()
        assert "ON CONFLICT" in sql and "EXCLUDED" in sql
        assert sql.count("%s") == len(FILING_COLUMNS)


class TestUpsertDocument:
    def test_sqlite_updates_every_document_column(self):
        sql = SQLITE.upsert_document_sql()
        for column in DOCUMENT_COLUMNS:
            assert f'"{column}" = ?' in sql
        assert sql.strip().endswith('WHERE "filing_id" = ?')

    def test_mysql_uses_percent_s_placeholders(self):
        sql = MYSQL.upsert_document_sql()
        for column in DOCUMENT_COLUMNS:
            assert f"`{column}` = %s" in sql
        assert sql.strip().endswith("WHERE `filing_id` = %s")


class TestCoverageAndEdges:
    def test_coverage_conflict_key(self):
        sql = SQLITE.insert_coverage_sql()
        assert 'ON CONFLICT ("chunk_from", "chunk_to", "run_id") DO UPDATE' in sql
        assert sql.count("?") == len(COVERAGE_COLUMNS)

    def test_mysql_edge_uses_insert_ignore(self):
        assert MYSQL.upsert_edge_sql("has_filing").startswith("INSERT IGNORE INTO `has_filing`")
        assert MYSQL.upsert_edge_sql("references_filing").startswith(
            "INSERT IGNORE INTO `references_filing`"
        )

    def test_sqlite_edge_conflict_noop(self):
        sql = SQLITE.upsert_edge_sql("has_filing")
        assert 'ON CONFLICT ("company_id", "filing_id") DO NOTHING' in sql
        ref = SQLITE.upsert_edge_sql("references_filing")
        assert 'ON CONFLICT ("filing_id", "company_id") DO NOTHING' in ref


class TestDdl:
    def test_every_relational_dialect_is_idempotent(self):
        for dialect in (MYSQL, MARIADB, SQLITE):
            joined = "\n".join(dialect.ddl())
            assert "IF NOT EXISTS" in joined
            for table in ("exchange_filing", "scrape_coverage", "has_filing", "references_filing"):
                assert table in joined

    def test_sqlite_indexes_target_the_right_tables(self):
        joined = "\n".join(SQLITE.ddl())
        assert 'CREATE INDEX IF NOT EXISTS "idx_sq_ef_ticker" ON "exchange_filing"' in joined
        assert 'CREATE INDEX IF NOT EXISTS "idx_sq_cov_run" ON "scrape_coverage"' in joined
        assert 'CREATE INDEX IF NOT EXISTS "idx_sq_hf_filing" ON "has_filing"' in joined
        assert 'CREATE INDEX IF NOT EXISTS "idx_sq_rf_company" ON "references_filing"' in joined

    def test_mysql_indexes_are_inline(self):
        joined = "\n".join(MYSQL.ddl())
        assert "CREATE INDEX" not in joined
        assert "KEY `idx_my_ef_ticker`" in joined
        assert "KEY `idx_my_cov_run`" in joined
        assert "ENGINE=InnoDB" in joined

    def test_postgres_ddl_is_idempotent(self):
        joined = "\n".join(POSTGRES.ddl())
        assert "IF NOT EXISTS" in joined
        assert "bigserial" in joined


class TestValueEncoding:
    def test_sqlite_encodes_json_and_datetimes_as_text(self):
        from datetime import datetime, timezone

        value = SQLITE.encode(["0700.HK"])
        assert isinstance(value, str) and value.startswith("[")
        stamp = SQLITE.encode(datetime(2024, 7, 1, tzinfo=timezone.utc))
        assert isinstance(stamp, str) and stamp.startswith("2024-07-01")

    def test_mysql_encodes_json_but_keeps_datetimes(self):
        from datetime import datetime, timezone

        assert MYSQL.encode({"a": 1}) == '{"a": 1}'
        stamp = datetime(2024, 7, 1, tzinfo=timezone.utc)
        assert MYSQL.encode(stamp) is stamp
