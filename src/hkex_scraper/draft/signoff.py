"""Human sign-off gate (A-006).

A draft moves pending → approved only through an explicit human approval;
sending is refused until approved.
"""

from __future__ import annotations

from .composer import OutreachDraft


class InvalidTransitionError(Exception):
    pass


def approve(draft: OutreachDraft, by: str, at: str) -> None:
    if draft.status != "pending":
        raise InvalidTransitionError(f"cannot approve a {draft.status} draft")
    draft.status = "approved"
    draft.signoff_by = by
    draft.signoff_at = at
