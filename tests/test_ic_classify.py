"""Tests for internal-control trigger classification (A-004)."""

from __future__ import annotations

from hkex_scraper.classify import classify_internal_control


def test_qualified_opinion_is_critical():
    r = classify_internal_control("Qualified Opinion in the Annual Results")
    assert r.ic_required is True
    assert r.severity == "Critical"
    assert "qualified_opinion" in r.signals
    assert r.escalated is True


def test_auditor_resignation_is_critical():
    r = classify_internal_control("Resignation of Auditor")
    assert r.severity == "Critical"
    assert "auditor_resignation" in r.signals


def test_material_weakness_restatement_sfc_are_high():
    for title, sig in (
        ("Material Internal Control Weakness", "material_ic_weakness"),
        ("Financial Restatement Announcement", "restatement"),
        ("SFC Enforcement Action", "sfc_action"),
    ):
        r = classify_internal_control(title)
        assert r.severity == "High"
        assert sig in r.signals


def test_single_medium_signal_does_not_escalate():
    r = classify_internal_control("Resignation of Chief Financial Officer")
    assert r.severity == "Medium"
    assert "personnel_change" in r.signals
    assert r.escalated is False


def test_two_medium_signals_escalate():
    r = classify_internal_control(
        "Change of Chief Financial Officer and Non-compliance with the Corporate Governance Code"
    )
    assert "personnel_change" in r.signals
    assert "cg_code_deficiency" in r.signals
    assert r.escalated is True


def test_bilingual_signals():
    assert classify_internal_control("保留意见").severity == "Critical"
    assert classify_internal_control("辞任核数师").severity == "Critical"


def test_insufficient_text():
    r = classify_internal_control("", "")
    assert r.ic_required is False
    assert r.severity == "Low"
    assert "insufficient text" in r.reasons


def test_no_signal():
    r = classify_internal_control("Annual Results Announcement")
    assert r.ic_required is False
    assert r.severity == "Low"
