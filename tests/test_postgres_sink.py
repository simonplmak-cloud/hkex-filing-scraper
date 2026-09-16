"""Unit tests for the optional PostgreSQL dual-store sink.

Pure unit tests — no database or network required. The generated DDL and SQL
are asserted directly, and the write control flow is exercised with the driver
availability and connection helpers monkeypatched.
"""

from __future__ import annotations

from hkex_scraper import config, db_postgres, pipeline


# ---------------------------------------------------------------------------
# AC-1 / AC-8: destination selection and CLI override
# ---------------------------------------------------------------------------


class TestDestinationSelection:
    def test_default_is_surrealdb_only(self):
        assert config._resolve_sinks("") == (True, False)
        assert config._resolve_sinks("surrealdb") == (True, False)

    def test_postgres_only(self):
        for value in ("postgres", "postgresql", "pg"):
            assert config._resolve_sinks(value) == (False, True)

    def test_both(self):
        for value in ("both", "dual"):
            assert config._resolve_sinks(value) == (True, True)

    def test_cli_override_precedence(self):
        assert config.apply_database_target("postgres") == (False, True)
        assert config.surrealdb_enabled() is False
        assert config.postgres_enabled() is True
        assert config.apply_database_target("both") == (True, True)
        assert config.apply_database_target("surrealdb") == (True, False)
        assert config.surrealdb_enabled() is True
        assert config.postgres_enabled() is False


# ---------------------------------------------------------------------------
# AC-2: idempotent schema DDL
# ---------------------------------------------------------------------------


class TestSchemaDdl:
    def test_schema_ddl_idempotent(self):
        statements = db_postgres._build_postgres_schema_sql()
        assert statements
        for stmt in statements:
            assert "IF NOT EXISTS" in stmt, stmt[:120]
        joined = "\n".join(statements)
        for table in (
            "exchange_filing",
            "scrape_coverage",
            "has_filing",
            "references_filing",
        ):
            assert f"TABLE IF NOT EXISTS {table}" in joined
        for column in ("filing_id", "document_text", "document_tables", "company_ticker"):
            assert column in joined
        for index in (
            "idx_pg_ef_ticker",
            "idx_pg_ef_docstatus",
            "idx_pg_ef_doc_tables",
            "idx_pg_cov_run",
        ):
            assert index in joined
        # exchange_filing must be created before the FK-bearing edge tables.
        assert joined.index("TABLE IF NOT EXISTS exchange_filing") < joined.index(
            "REFERENCES exchange_filing"
        )


# ---------------------------------------------------------------------------
# AC-3: filing metadata upsert
# ---------------------------------------------------------------------------


class TestUpsertFilings:
    def test_upsert_filings_sql_and_binds(self):
        sql = db_postgres._build_upsert_filings_sql()
        assert "INSERT INTO exchange_filing" in sql
        assert "ON CONFLICT (filing_id) DO UPDATE" in sql
        # Metadata upsert must NOT touch document payload columns.
        assert "document_text" not in sql
        assert "document_tables" not in sql
        assert sql.count("%s") == len(db_postgres._FILING_COLUMNS)

    def test_filing_row_shape(self):
        row = db_postgres._filing_row(
            {
                "filing_id": "abc123",
                "company_ticker": "0451.HK",
                "stock_code": "0451",
                "filing_type": "Annual Report",
            }
        )
        assert len(row) == len(db_postgres._FILING_COLUMNS)
        assert row[0] == "abc123"
        assert row[1] == "0451.HK"
        assert row[4] == "HK"
        assert row[12] == "HKEx"


# ---------------------------------------------------------------------------
# AC-4: document payload upsert and truncation parity
# ---------------------------------------------------------------------------


class TestUpsertDocument:
    def test_upsert_document_sql_and_truncation(self, monkeypatch):
        sql = db_postgres._build_upsert_document_sql()
        for column in db_postgres._DOCUMENT_COLUMNS:
            assert f"{column} = %s" in sql
        assert "WHERE filing_id = %s" in sql

        captured = {}

        def fake_run(statement, params=None, many=False):
            captured["sql"] = statement
            captured["params"] = params
            return True, db_postgres.ERR_NONE, 1

        monkeypatch.setattr(db_postgres, "_PSYCOPG_AVAILABLE", True)
        monkeypatch.setattr(db_postgres, "postgres_conninfo", lambda: "host=x")
        monkeypatch.setattr(db_postgres, "_run", fake_run)

        truncated = "head-of-text"
        ok, code = db_postgres.upsert_document(
            "fid1",
            {
                "document_size": 10,
                "document_type": "pdf",
                "document_hash": "h",
                "document_text": truncated,
                "document_text_len": len(truncated),
                "document_tables": [{"tableIndex": 0}],
                "document_table_cnt": 1,
                "document_status": "processed",
                "document_status_reason": "",
            },
        )
        assert ok is True
        assert code == db_postgres.ERR_NONE
        # The caller's truncation decision is passed through unchanged.
        assert captured["params"][3] == truncated
        assert captured["params"][4] == len(truncated)

    def test_upsert_document_missing_row_is_failure(self, monkeypatch):
        monkeypatch.setattr(db_postgres, "_PSYCOPG_AVAILABLE", True)
        monkeypatch.setattr(db_postgres, "postgres_conninfo", lambda: "host=x")
        monkeypatch.setattr(db_postgres, "_run", lambda *a, **k: (True, db_postgres.ERR_NONE, 0))
        ok, code = db_postgres.upsert_document("missing", {"document_status": "processed"})
        assert ok is False
        assert code == db_postgres.ERR_WRITE_ERROR


# ---------------------------------------------------------------------------
# AC-5: coverage upsert
# ---------------------------------------------------------------------------


class TestInsertCoverage:
    def test_insert_coverage_conflict_key(self):
        sql = db_postgres._build_insert_coverage_sql()
        assert "INSERT INTO scrape_coverage" in sql
        assert "ON CONFLICT (chunk_from, chunk_to, run_id) DO UPDATE" in sql
        assert sql.count("%s") == len(db_postgres._COVERAGE_COLUMNS)

    def test_coverage_row_shape(self):
        row = db_postgres._coverage_row(
            {
                "chunk_from": 1,
                "chunk_to": 2,
                "api_count": 3,
                "ingested_count": 4,
                "unique_count": 5,
                "run_id": "run",
            }
        )
        assert row[:6] == (1, 2, 3, 4, 5, "run")


# ---------------------------------------------------------------------------
# AC-6: graph edges
# ---------------------------------------------------------------------------


class TestUpsertEdges:
    def test_upsert_edges_conflict_noop(self):
        has_filing_sql = db_postgres._build_upsert_edges_sql("has_filing")
        assert "INSERT INTO has_filing" in has_filing_sql
        assert "ON CONFLICT (company_id, filing_id) DO NOTHING" in has_filing_sql

        ref_sql = db_postgres._build_upsert_edges_sql("references_filing")
        assert "INSERT INTO references_filing" in ref_sql
        assert "ON CONFLICT (filing_id, company_id) DO NOTHING" in ref_sql

    def test_unknown_edge_kind_is_payload_error(self, monkeypatch):
        monkeypatch.setattr(db_postgres, "_PSYCOPG_AVAILABLE", True)
        monkeypatch.setattr(db_postgres, "postgres_conninfo", lambda: "host=x")
        created, code = db_postgres.upsert_edges(
            [{"company_id": "1_HK", "filing_id": "f"}], "bogus"
        )
        assert created == 0
        assert code == db_postgres.ERR_PAYLOAD_ERROR


# ---------------------------------------------------------------------------
# AC-7 / AC-E2: graceful degradation
# ---------------------------------------------------------------------------


class TestGracefulDegradation:
    def test_graceful_degradation_driver_missing(self, monkeypatch):
        monkeypatch.setattr(db_postgres, "_PSYCOPG_AVAILABLE", False)
        assert db_postgres.postgres_available() is False
        assert db_postgres.upsert_filings([{"filing_id": "x"}]) == (
            0,
            db_postgres.ERR_DRIVER_MISSING,
        )
        ok, code = db_postgres.initialize_postgres_schema()
        assert ok is False
        assert code == db_postgres.ERR_DRIVER_MISSING
        ok, code = db_postgres.upsert_document("x", {})
        assert ok is False
        assert code == db_postgres.ERR_DRIVER_MISSING
        # No exception raised anywhere above.

    def test_dsn_missing_degrades(self, monkeypatch):
        monkeypatch.setattr(db_postgres, "_PSYCOPG_AVAILABLE", True)
        monkeypatch.setattr(db_postgres, "postgres_conninfo", lambda: "")
        assert db_postgres.postgres_available() is False
        assert db_postgres.upsert_filings([{"filing_id": "x"}]) == (
            0,
            db_postgres.ERR_DSN_MISSING,
        )
        ok, code = db_postgres.initialize_postgres_schema()
        assert ok is False
        assert code == db_postgres.ERR_DSN_MISSING


# ---------------------------------------------------------------------------
# AC-E3: credentials never logged
# ---------------------------------------------------------------------------


class TestCredentialRedaction:
    def test_dsn_never_logged(self):
        redacted = db_postgres._redact(
            "connection failed: host=db password=supersecret123 user=simon"
        )
        assert "supersecret123" not in redacted
        assert "***" in redacted

    def test_url_credentials_redacted(self):
        redacted = db_postgres._redact(
            "could not connect to postgresql://simon:topsecret@db.local:5432/hkex"
        )
        assert "topsecret" not in redacted
        assert "***" in redacted


# ---------------------------------------------------------------------------
# AC-E1 / AC-19: failure isolation and counters
# ---------------------------------------------------------------------------


def _sample_filing() -> dict:
    return {
        "stockCode": "0451",
        "date": "01/07/2024",
        "title": "Annual Report 2024",
        "stockName": "Example Holdings",
        "link": "https://www1.hkexnews.hk/example.pdf",
    }


class TestFailureIsolation:
    def test_failure_isolation_and_counters(self, monkeypatch):
        pipeline.reset_sink_stats()
        monkeypatch.setattr(config, "surrealdb_enabled", lambda: True)
        monkeypatch.setattr(config, "postgres_enabled", lambda: True)
        monkeypatch.setattr(config, "postgres_required", lambda: False)

        # SurrealDB succeeds; PostgreSQL fails.
        monkeypatch.setattr(pipeline, "upsert_batch_with_retry", lambda stmts: len(stmts))
        monkeypatch.setattr(db_postgres, "postgres_available", lambda: True)
        monkeypatch.setattr(
            db_postgres,
            "upsert_filings",
            lambda records: (0, db_postgres.ERR_WRITE_ERROR),
        )

        saved = pipeline._save_filings_batch_metadata([_sample_filing()])
        assert saved == 1  # the SurrealDB write is retained
        assert pipeline.SINK_STATS["surrealdb"]["ok"] == 1
        assert pipeline.SINK_STATS["postgres"]["failed"] == 1
        # Optional sink failure does not fail the run.
        assert pipeline.sink_exit_code() == 0

    def test_postgres_only_required_failure_exits_nonzero(self, monkeypatch):
        pipeline.reset_sink_stats()
        monkeypatch.setattr(config, "surrealdb_enabled", lambda: False)
        monkeypatch.setattr(config, "postgres_enabled", lambda: True)
        monkeypatch.setattr(config, "postgres_required", lambda: True)
        monkeypatch.setattr(db_postgres, "postgres_available", lambda: True)
        monkeypatch.setattr(
            db_postgres,
            "upsert_filings",
            lambda records: (0, db_postgres.ERR_WRITE_ERROR),
        )

        saved = pipeline._save_filings_batch_metadata([_sample_filing()])
        assert saved == 1
        assert pipeline.SINK_STATS["postgres"]["failed"] == 1
        assert pipeline.sink_exit_code() == 1

    def test_unavailable_postgres_logs_and_continues(self, monkeypatch, capsys):
        pipeline.reset_sink_stats()
        monkeypatch.setattr(pipeline, "_pg_unavailable_warned", False)
        monkeypatch.setattr(config, "surrealdb_enabled", lambda: True)
        monkeypatch.setattr(config, "postgres_enabled", lambda: True)
        monkeypatch.setattr(config, "postgres_required", lambda: False)
        monkeypatch.setattr(pipeline, "upsert_batch_with_retry", lambda stmts: len(stmts))
        monkeypatch.setattr(db_postgres, "postgres_available", lambda: False)
        monkeypatch.setattr(db_postgres, "driver_installed", lambda: False)

        saved = pipeline._save_filings_batch_metadata([_sample_filing()])
        out = capsys.readouterr().out
        assert saved == 1
        assert "psycopg" in out
        assert pipeline.SINK_STATS["surrealdb"]["ok"] == 1


# ---------------------------------------------------------------------------
# AC-9: parity report
# ---------------------------------------------------------------------------


class TestParityReport:
    def test_parity_report_delta(self):
        from hkex_scraper.main import _format_parity_report

        ok_report = _format_parity_report(10, 10)
        assert "Parity: OK" in ok_report
        assert "WARNING" not in ok_report

        mismatch = _format_parity_report(10, 8)
        assert "WARNING" in mismatch
        assert db_postgres.ERR_PARITY_MISMATCH in mismatch
        assert "Difference:        2" in mismatch

    def test_parity_report_na_when_surrealdb_disabled(self):
        from hkex_scraper.main import _format_parity_report

        report = _format_parity_report(0, 8, surrealdb_enabled=False)
        assert "Parity: N/A" in report
        assert "WARNING" not in report
        assert "PostgreSQL filings: 8" in report


# ---------------------------------------------------------------------------
# AC-E2: required-sink fail-fast
# ---------------------------------------------------------------------------


class TestRequiredSinkFailFast:
    def test_driver_missing_is_reported(self, monkeypatch):
        from hkex_scraper import main

        monkeypatch.setattr(config, "postgres_required", lambda: True)
        monkeypatch.setattr(db_postgres, "driver_installed", lambda: False)
        error = main._required_sink_error()
        assert "psycopg" in error
        assert "postgres" in error

    def test_conninfo_missing_is_reported(self, monkeypatch):
        from hkex_scraper import main

        monkeypatch.setattr(config, "postgres_required", lambda: True)
        monkeypatch.setattr(db_postgres, "driver_installed", lambda: True)
        monkeypatch.setattr(config, "postgres_conninfo", lambda: "")
        error = main._required_sink_error()
        assert "POSTGRES_DSN" in error

    def test_usable_required_sink_has_no_error(self, monkeypatch):
        from hkex_scraper import main

        monkeypatch.setattr(config, "postgres_required", lambda: True)
        monkeypatch.setattr(db_postgres, "driver_installed", lambda: True)
        monkeypatch.setattr(config, "postgres_conninfo", lambda: "postgresql://x")
        assert main._required_sink_error() == ""

    def test_optional_sink_never_errors(self, monkeypatch):
        from hkex_scraper import main

        monkeypatch.setattr(config, "postgres_required", lambda: False)
        monkeypatch.setattr(db_postgres, "driver_installed", lambda: False)
        assert main._required_sink_error() == ""
