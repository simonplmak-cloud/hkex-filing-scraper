"""Tests for outreach drafting + human-gated send (A-006)."""

from __future__ import annotations

import pytest

from hkex_scraper.draft import (
    EmailSender,
    InvalidRecipientError,
    InvalidTransitionError,
    NotApprovedError,
    SendError,
    approve,
    compose_draft,
)


def _opportunity(**overrides):
    base = {
        "stock_code": "00700",
        "company_name": "ABC Corporation Limited",
        "company_name_c": "ABC集团",
        "company_address": "Room 1, 10/F, Central Plaza, Hong Kong",
        "company_website": "",
        "finance_executive": {
            "name": "Joe Doe",
            "title": "Chief Financial Officer",
            "email": "cfo@abc.com",
        },
        "corporate_action": {
            "title": "Very Substantial Acquisition",
            "url": "https://example.com/f.pdf",
        },
        "file_date": "2026-09-16",
    }
    base.update(overrides)
    return base


def test_compose_draft_subject_and_body():
    d = compose_draft(_opportunity(), "filing-1")
    assert "ABC Corporation Limited" in d.subject
    assert "Joe Doe" in d.body
    assert "Very Substantial Acquisition" in d.body
    assert d.recipient_email == "cfo@abc.com"
    assert d.status == "pending"
    assert d.send_key


def test_compose_draft_leaves_missing_fields_blank():
    d = compose_draft(
        _opportunity(company_name="", finance_executive={"name": "", "title": "", "email": ""}),
        "f-2",
    )
    assert d.company_name == ""
    assert d.recipient_email == ""
    assert d.body.startswith("Dear Sir/Madam")
    # no fabricated website
    assert d.company_website == ""


def test_approve_moves_pending_to_approved():
    d = compose_draft(_opportunity(), "f-3")
    approve(d, "simon", "2026-09-17T08:00:00Z")
    assert d.status == "approved"
    assert d.signoff_by == "simon"


def test_approve_twice_raises():
    d = compose_draft(_opportunity(), "f-4")
    approve(d, "simon", "2026-09-17T08:00:00Z")
    with pytest.raises(InvalidTransitionError):
        approve(d, "simon", "2026-09-17T09:00:00Z")


def test_send_requires_approval():
    sender = EmailSender(transport=lambda d: None)
    d = compose_draft(_opportunity(), "f-5")
    with pytest.raises(NotApprovedError):
        sender.send(d)


def test_send_is_idempotent():
    sent = []
    sender = EmailSender(transport=lambda d: sent.append(d.send_key))
    d = compose_draft(_opportunity(), "f-6")
    approve(d, "simon", "2026-09-17T08:00:00Z")
    assert sender.send(d) is True
    assert d.status == "sent"
    assert sender.send(d) is False  # duplicate → no-op
    assert len(sent) == 1


def test_send_rejects_invalid_recipient():
    sender = EmailSender(transport=lambda d: None)
    d = compose_draft(
        _opportunity(finance_executive={"name": "Joe", "title": "CFO", "email": ""}), "f-7"
    )
    approve(d, "simon", "2026-09-17T08:00:00Z")
    with pytest.raises(InvalidRecipientError):
        sender.send(d)


def test_send_surfaces_transport_failure():
    def boom(_d):
        raise SendError("send failed")

    sender = EmailSender(transport=boom)
    d = compose_draft(_opportunity(), "f-8")
    approve(d, "simon", "2026-09-17T08:00:00Z")
    with pytest.raises(SendError):
        sender.send(d)
