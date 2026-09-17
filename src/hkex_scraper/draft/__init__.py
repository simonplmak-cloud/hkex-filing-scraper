"""Outreach drafting + human-gated send (A-006)."""

from .composer import OutreachDraft, compose_draft
from .sender import EmailSender, InvalidRecipientError, NotApprovedError, SendError
from .signoff import InvalidTransitionError, approve

__all__ = [
    "EmailSender",
    "InvalidRecipientError",
    "InvalidTransitionError",
    "NotApprovedError",
    "OutreachDraft",
    "SendError",
    "approve",
    "compose_draft",
]
