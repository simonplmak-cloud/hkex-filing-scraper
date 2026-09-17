"""Tests for sustainability (ESG) trigger classification (A-005)."""

from __future__ import annotations

from hkex_scraper.classify import classify_scope_gap, classify_sustainability


def test_largecap_missing_scope12_is_tier1():
    r = classify_scope_gap("LargeCap", has_scope12=False)
    assert r is not None
    assert r.esg_required is True
    assert r.tier == 1
    assert "missing_mandatory_scope12" in r.signals
    assert classify_scope_gap("LargeCap", has_scope12=True) is None


def test_green_claims_are_tier1():
    r = classify_sustainability("Issuance of Green Bond")
    assert r.esg_required is True
    assert r.tier == 1
    assert "green_finance_claims" in r.signals


def test_esg_reporting_tier_by_segment():
    assert classify_sustainability("ESG Report 2024", issuer_segment="Main Board").tier == 2
    assert classify_sustainability("ESG Report 2024", issuer_segment="GEM").tier == 3


def test_transition_planning_is_tier3():
    r = classify_sustainability("Transition Plan Announcement")
    assert r.esg_required is True
    assert r.tier == 3
    assert "transition_planning" in r.signals


def test_non_esg_filing_not_flagged():
    r = classify_sustainability("Annual Results for the Year Ended 2024")
    assert r.esg_required is False


def test_bilingual():
    assert classify_sustainability("绿色债券发行").tier == 1


def test_insufficient_text():
    r = classify_sustainability("", "")
    assert r.esg_required is False
    assert "insufficient text" in r.reasons
