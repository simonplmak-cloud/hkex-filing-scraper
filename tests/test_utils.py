"""Unit tests for hkex_scraper.utils — no database or network required."""

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
        assert classify_filing("Annual Report 2024")[0] == "ANNUAL_REPORT"

    def test_interim_results(self):
        assert classify_filing("Interim Results Announcement")[0] == "RESULTS"

    def test_dividend(self):
        assert classify_filing("Payment of Dividend")[0] == "DIVIDEND"

    def test_other(self):
        assert classify_filing("Some random announcement")[0] == "OTHER"


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
