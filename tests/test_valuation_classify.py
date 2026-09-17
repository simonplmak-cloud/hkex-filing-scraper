"""Tests for valuation-trigger classification (A-003)."""

from __future__ import annotations

from hkex_scraper.classify import classify_valuation


def test_vsa_is_high_ch14():
    r = classify_valuation("Very Substantial Acquisition of Target Company")
    assert r.valuation_required is True
    assert r.confidence == "High"
    assert "very_substantial_acquisition_disposal" in r.triggers
    assert any("Ch.14" in reason for reason in r.reasons)


def test_connected_transaction_is_medium_ch14a():
    r = classify_valuation("Connected Transaction with a Substantial Shareholder")
    assert r.valuation_required is True
    assert r.confidence == "Medium"
    assert "connected_transaction" in r.triggers
    assert any("Ch.14A" in reason for reason in r.reasons)


def test_property_transaction_flagged_with_overlap():
    r = classify_valuation("Acquisition of Property in Central")
    assert r.valuation_required is True
    assert "property_transaction" in r.triggers
    # Overlapping flags: "Acquisition" also matches business_combination.
    assert len(r.triggers) >= 2


def test_share_based_payment_and_business_combination():
    r = classify_valuation("Grant of Share Options under the Share Option Scheme")
    assert r.valuation_required is True
    assert "share_based_payment" in r.triggers
    assert any("HKFRS 2" in reason for reason in r.reasons)

    r2 = classify_valuation("Business Combination involving a Target")
    assert r2.valuation_required is True
    assert "business_combination" in r2.triggers
    assert any("HKFRS 3" in reason for reason in r2.reasons)


def test_non_trigger_filings_not_flagged():
    for title in ("Annual Results for the Year Ended 31 December 2024", "Appointment of Director"):
        r = classify_valuation(title)
        assert r.valuation_required is False
        assert r.confidence == "High"


def test_bilingual_keywords():
    r = classify_valuation("非常重大收购事项")
    assert r.valuation_required is True
    assert r.confidence == "High"
    assert "very_substantial_acquisition_disposal" in r.triggers

    r2 = classify_valuation("委任董事")
    assert r2.valuation_required is False
    assert r2.confidence == "High"


def test_undecidable_reports_low_not_silent_false():
    r = classify_valuation("Framework Agreement Announcement")
    assert r.confidence == "Low"
    assert "undecidable — model unavailable" in r.reasons


def test_insufficient_text():
    r = classify_valuation("", "")
    assert r.confidence == "Low"
    assert "insufficient text" in r.reasons
