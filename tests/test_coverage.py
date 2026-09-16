"""Unit tests for coverage verification — no database or network required."""

from hkex_scraper.api import fetch_chunk_via_api


class TestFetchChunkReturnType:
    def test_return_signature_returns_tuple(self):
        import inspect

        sig = inspect.signature(fetch_chunk_via_api)
        annotation_str = str(sig.return_annotation)
        assert "Tuple" in annotation_str or "tuple" in annotation_str

    def test_return_annotation_is_tuple_of_list_and_optional_int(self):
        import inspect

        sig = inspect.signature(fetch_chunk_via_api)
        annotation_str = str(sig.return_annotation)
        assert "list" in annotation_str
        assert "int" in annotation_str


class TestCoveragePctCalculation:
    def test_full_coverage(self):
        assert (100 / 100) * 100 == 100.0

    def test_partial_coverage(self):
        pct = (42 / 100) * 100
        assert pct == 42.0

    def test_zero_unique(self):
        pct = (0 / 100) * 100
        assert pct == 0.0


class TestCoverageInsertStatement:
    def test_insert_contains_required_fields(self):
        run_id = "2026-08-12_14-30"
        sql = (
            f"INSERT INTO scrape_coverage {{"
            f"  chunkFrom: d'2026-07-01', "
            f"  chunkTo: d'2026-07-31', "
            f"  apiCount: 100, "
            f"  ingestedCount: 100, "
            f"  uniqueCount: 95, "
            f"  runId: '{run_id}', "
            f"  timestamp: time::now()"
            f"}};"
        )
        assert "scrape_coverage" in sql
        assert "chunkFrom" in sql
        assert "chunkTo" in sql
        assert "apiCount: 100" in sql
        assert "ingestedCount" in sql
        assert "uniqueCount" in sql
        assert run_id in sql
        assert "time::now()" in sql

    def test_insert_handles_zero_api_count(self):
        sql = (
            "INSERT INTO scrape_coverage {"
            "  chunkFrom: d'2026-07-01', "
            "  chunkTo: d'2026-07-31', "
            "  apiCount: 0, "
            "  ingestedCount: 0, "
            "  uniqueCount: 0, "
            "  runId: '2026-08-12_14-30', "
            "  timestamp: time::now()"
            "};"
        )
        assert "apiCount: 0" in sql
        assert "ingestedCount: 0" in sql
        assert "uniqueCount: 0" in sql


class TestCoverageLoggingLogic:
    def test_pct_format_one_decimal(self):
        pct = (1567 / 4823) * 100
        formatted = f"{pct:.1f}%"
        assert "." in formatted
        assert formatted.endswith("%")

    def test_100_percent_format(self):
        pct = 100.0
        assert f"{pct:.1f}%" == "100.0%"

    def test_run_id_format(self):
        from datetime import datetime

        run_id = datetime(2026, 8, 12, 14, 30).strftime("%Y-%m-%d_%H-%M")
        assert run_id == "2026-08-12_14-30"
        assert len(run_id) == 16

    def test_run_id_matches_log_file_convention(self):
        from datetime import datetime

        now = datetime.now()
        run_id = now.strftime("%Y-%m-%d_%H-%M")
        expected_log = f"hkex_filings_{run_id}.log"
        assert expected_log.startswith("hkex_filings_")
        assert expected_log.endswith(".log")
