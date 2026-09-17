"""Email sender (A-006).

Sends an approved draft via an injectable transport (default: a Microsoft Graph
`sendMail` POST over `requests`). Sending is idempotent per `send_key` and
refuses drafts that are not approved or lack a valid recipient.
"""

from __future__ import annotations

import os
import time
from typing import Callable

from .composer import OutreachDraft

MAX_TRIES = 3
WAIT_SECONDS = 5

Transport = Callable[[OutreachDraft], None]


class NotApprovedError(Exception):
    pass


class InvalidRecipientError(Exception):
    pass


class SendError(Exception):
    pass


def _default_transport(draft: OutreachDraft) -> None:
    """Send via Microsoft Graph `sendMail` using a bearer token from env."""
    import requests

    token = os.environ.get("OUTLOOK_GRAPH_TOKEN", "")
    if not token:
        raise SendError("OUTLOOK_GRAPH_TOKEN is not configured")
    url = "https://graph.microsoft.com/v1.0/me/sendMail"
    payload = {
        "message": {
            "subject": draft.subject,
            "body": {"contentType": "Text", "content": draft.body},
            "toRecipients": [{"emailAddress": {"address": draft.recipient_email}}],
        }
    }
    last: Exception | None = None
    for attempt in range(1, MAX_TRIES + 1):
        try:
            resp = requests.post(
                url, json=payload, headers={"Authorization": f"Bearer {token}"}, timeout=30
            )
            if resp.status_code in (200, 202):
                return
            if resp.status_code < 500:
                raise SendError(f"send failed: HTTP {resp.status_code}")
            last = SendError(f"send failed: HTTP {resp.status_code}")
        except SendError as exc:
            last = exc
        if attempt < MAX_TRIES:
            time.sleep(WAIT_SECONDS)
    raise SendError(f"send failed after {MAX_TRIES} attempts") from last


class EmailSender:
    def __init__(self, transport: Transport | None = None):
        self._transport = transport or _default_transport
        self._sent: set[str] = set()

    def send(self, draft: OutreachDraft) -> bool:
        if draft.status != "approved":
            raise NotApprovedError(f"draft {draft.id or draft.send_key[:8]} is not approved")
        if not draft.recipient_email or "@" not in draft.recipient_email:
            raise InvalidRecipientError("recipient email is empty or malformed")
        if draft.send_key in self._sent:
            return False  # idempotent no-op
        self._transport(draft)
        self._sent.add(draft.send_key)
        draft.status = "sent"
        return True
