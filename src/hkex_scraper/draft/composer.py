"""Draft composer (A-006).

Builds an outreach letter (subject, postal to-address, body) from an
opportunity record. Empty fields stay empty — nothing is fabricated.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Mapping

# status: pending | approved | sent | failed
VALID_STATUSES = ("pending", "approved", "sent", "failed")


@dataclass(frozen=True)
class FinanceExecutive:
    name: str
    title: str
    email: str


@dataclass(frozen=True)
class CorporateAction:
    title: str
    url: str


@dataclass
class OutreachDraft:
    filing_id: str
    stock_code: str
    company_name: str
    company_name_c: str
    company_address: str
    company_website: str
    finance_executive: FinanceExecutive
    corporate_action: CorporateAction
    file_date: str
    subject: str = ""
    to_address: str = ""
    recipient_email: str = ""
    body: str = ""
    status: str = "pending"
    send_key: str = ""
    id: str = ""
    signoff_by: str = ""
    signoff_at: str = ""


def _str(value: object) -> str:
    return str(value).strip() if value is not None else ""


def _build_body(company: str, ca_title: str, file_date: str, fe_name: str) -> str:
    greeting = f"Dear {fe_name}," if fe_name else "Dear Sir/Madam,"
    parts = [greeting]
    if company and ca_title and file_date:
        parts.append(
            f'We note {company}\'s recent initiative "{ca_title}", announced on {file_date}.'
        )
    elif company and ca_title:
        parts.append(f'We note {company}\'s recent initiative "{ca_title}".')
    elif company:
        parts.append(f"We note a recent initiative by {company}.")
    parts.append(
        "Ascent Partners is a leading independent valuation and advisory firm with 17+ years "
        "of experience serving HKEX-listed companies and private equity firms across Asia."
    )
    parts.append(
        "We would welcome the opportunity to discuss how we can support this initiative with "
        "tailored valuation and advisory services."
    )
    return "\n\n".join(parts)


def compose_draft(opportunity: Mapping[str, Any], filing_id: str) -> OutreachDraft:
    company = _str(opportunity.get("company_name"))
    fe = opportunity.get("finance_executive") or {}
    fe_name = _str(fe.get("name"))
    fe_title = _str(fe.get("title"))
    fe_email = _str(fe.get("email"))
    ca = opportunity.get("corporate_action") or {}
    ca_title = _str(ca.get("title"))
    ca_url = _str(ca.get("url"))
    file_date = _str(opportunity.get("file_date"))

    subject = (
        f"Valuation Support for {company} | Ascent Partners"
        if company
        else "Valuation Support | Ascent Partners"
    )

    to_lines = [line for line in (_str(opportunity.get("company_address")),) if line]
    if company:
        to_lines.insert(0, company)
    attention = fe_name if not fe_title else f"{fe_name}, {fe_title}"
    if attention:
        to_lines.append(f"Attention to: {attention}")
    to_address = "\n".join(to_lines)

    body = _build_body(company, ca_title, file_date, fe_name)
    send_key = hashlib.sha256(f"{filing_id}|{company}".encode("utf-8")).hexdigest()

    return OutreachDraft(
        filing_id=filing_id,
        stock_code=_str(opportunity.get("stock_code")),
        company_name=company,
        company_name_c=_str(opportunity.get("company_name_c")),
        company_address=_str(opportunity.get("company_address")),
        company_website=_str(opportunity.get("company_website")),
        finance_executive=FinanceExecutive(fe_name, fe_title, fe_email),
        corporate_action=CorporateAction(ca_title, ca_url),
        file_date=file_date,
        subject=subject,
        to_address=to_address,
        recipient_email=fe_email,
        body=body,
        status="pending",
        send_key=send_key,
    )
