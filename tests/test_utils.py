"""Unit tests for hkex_scraper.utils — no database or network required."""

from hkex_scraper.graph import _normalize_company_id, _ticker_to_record_id
from hkex_scraper.utils import (
    classify_filing,
    escape_sql,
    extract_referenced_tickers,
    squash_ws,
)


class TestSquashWs:
    def test_collapses_whitespace(self):
        assert squash_ws("  hello   world  ") == "hello world"

    def test_removes_control_chars(self):
        assert squash_ws("hello\x00world") == "hello world"

    def test_empty_string(self):
        assert squash_ws("") == ""


class TestEscapeSql:
    def test_escapes_single_quotes(self):
        assert escape_sql("it's") == "it\\'s"

    def test_escapes_backslash(self):
        assert escape_sql("a\\b") == "a\\\\b"

    def test_none_returns_empty(self):
        assert escape_sql(None) == ""


class TestClassifyFiling:
    def test_annual_report(self):
        assert classify_filing("Annual Report 2024")[0] == "Annual Report"

    def test_interim_results(self):
        assert classify_filing("Interim Results Announcement")[0] == "Interim Results"

    def test_dividend(self):
        assert classify_filing("Payment of Dividend")[0] == "Dividend"

    def test_other(self):
        assert classify_filing("Some random announcement")[0] == "Other"

    def test_quarterly(self):
        assert classify_filing("Quarterly Report Q1 2025")[0] == "Quarterly"

    def test_meeting(self):
        assert classify_filing("Notice of Annual General Meeting")[0] == "Meeting"

    def test_transaction(self):
        assert classify_filing("Connected Transaction Announcement")[0] == "Transaction"

    def test_director(self):
        assert classify_filing("Change of Director")[0] == "Director"

    def test_circular(self):
        assert classify_filing("Proxy Form Circular")[0] == "Circular"


class TestExtractReferencedTickers:
    def test_extracts_stock_codes(self):
        title = "Joint Announcement (stock code: 0298, 1234)"
        result = extract_referenced_tickers(title, "0001")
        assert "0298.HK" in result
        assert "1234.HK" in result

    def test_excludes_own_code(self):
        title = "Joint Announcement (stock code: 0001, 0298)"
        result = extract_referenced_tickers(title, "0001")
        assert "0001.HK" not in result
        assert "0298.HK" in result

    def test_hk_suffix_pattern(self):
        title = "Regarding (0451.HK) and (0700.HK)"
        result = extract_referenced_tickers(title, "9999")
        assert "0451.HK" in result
        assert "0700.HK" in result

    def test_no_references(self):
        title = "Annual Report 2024"
        result = extract_referenced_tickers(title, "0001")
        assert result == []


class TestCompanyIdNormalization:
    def test_strips_leading_zeros(self):
        assert _ticker_to_record_id("0001.HK") == "1_HK"

    def test_six_digit_unpadded(self):
        assert _ticker_to_record_id("000426.HK") == "426_HK"

    def test_no_leading_zeros_unchanged(self):
        assert _ticker_to_record_id("2378.HK") == "2378_HK"

    def test_non_hk_exchange(self):
        assert _ticker_to_record_id("AAPL.US") == "AAPL_US"

    def test_normalize_db_id(self):
        assert _normalize_company_id("eodhd_company:0001_HK") == "1_HK"

    def test_normalize_db_id_six_digit(self):
        assert _normalize_company_id("eodhd_company:000426_HK") == "426_HK"

    def test_normalize_db_id_no_zeros(self):
        assert _normalize_company_id("eodhd_company:2378_HK") == "2378_HK"

    def test_ticker_and_db_id_match(self):
        assert _ticker_to_record_id("0001.HK") == _normalize_company_id(
            "eodhd_company:0001_HK"
        )
        assert _ticker_to_record_id("000426.HK") == _normalize_company_id(
            "eodhd_company:000426_HK"
        )
        assert _ticker_to_record_id("2378.HK") == _normalize_company_id(
            "eodhd_company:2378_HK"
        )
