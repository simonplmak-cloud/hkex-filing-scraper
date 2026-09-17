"""Sustainability (ESG/climate) trigger classification (A-005).

Tiered 1–3: unsupported green/net-zero claims are Tier 1; ESG/climate reporting
is Tier 2 (Tier 3 for GEM); early transition planning is Tier 3. The "missing
mandatory Scope 1/2" Tier-1 signal is data-driven and exposed via
`classify_scope_gap`.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ESGClassification:
    esg_required: bool
    tier: int  # 1 | 2 | 3
    signals: tuple[str, ...]
    issuer_segment: str
    reasons: tuple[str, ...]
    via: str


TIER1_KEYWORDS_EN = (
    "GREEN BOND",
    "SUSTAINABILITY-LINKED",
    "NET ZERO",
    "CARBON NEUTRAL",
    "GREEN FINANCING",
    "SUSTAINABLE FINANCE",
    "GREEN LOAN",
)
TIER1_KEYWORDS_ZH = (
    "绿色债券",
    "可持续发展挂钩",
    "净零",
    "碳中和",
    "绿色融资",
    "可持续发展融资",
    "绿色贷款",
)

TIER2_KEYWORDS_EN = (
    "ESG REPORT",
    "ENVIRONMENTAL SOCIAL AND GOVERNANCE",
    "SUSTAINABILITY REPORT",
    "CLIMATE DISCLOSURE",
)
TIER2_KEYWORDS_ZH = ("环境、社会及管治", "环境社会及管治", "可持续发展报告", "气候披露")

TIER3_KEYWORDS_EN = ("TRANSITION PLAN", "ESG DATA", "CARBON FOOTPRINT")
TIER3_KEYWORDS_ZH = ("转型计划", "碳足迹")


def classify_sustainability(
    title: str,
    description: str = "",
    issuer_segment: str = "Main Board",
) -> ESGClassification:
    text = f"{title or ''} {description or ''}".strip()
    if not text:
        return ESGClassification(False, 3, (), issuer_segment, ("insufficient text",), "rules")

    text_upper = text.upper()
    if any(k in text_upper for k in TIER1_KEYWORDS_EN) or any(k in text for k in TIER1_KEYWORDS_ZH):
        return ESGClassification(
            True,
            1,
            ("green_finance_claims",),
            issuer_segment,
            ("unsupported green/net-zero claim (Tier 1)",),
            "rules",
        )
    if any(k in text_upper for k in TIER2_KEYWORDS_EN) or any(k in text for k in TIER2_KEYWORDS_ZH):
        tier = 2 if issuer_segment != "GEM" else 3
        return ESGClassification(
            True,
            tier,
            ("esg_reporting",),
            issuer_segment,
            (f"ESG/climate reporting (Tier {tier})",),
            "rules",
        )
    if any(k in text_upper for k in TIER3_KEYWORDS_EN) or any(k in text for k in TIER3_KEYWORDS_ZH):
        return ESGClassification(
            True,
            3,
            ("transition_planning",),
            issuer_segment,
            ("early transition planning (Tier 3)",),
            "rules",
        )
    return ESGClassification(False, 3, (), issuer_segment, ("no ESG signal",), "rules")


def classify_scope_gap(issuer_segment: str, has_scope12: bool) -> ESGClassification | None:
    """Data-driven Tier-1 signal: a LargeCap issuer missing mandatory Scope 1/2."""
    if issuer_segment == "LargeCap" and not has_scope12:
        return ESGClassification(
            True,
            1,
            ("missing_mandatory_scope12",),
            issuer_segment,
            ("missing mandatory Scope 1/2 (Tier 1)",),
            "rules",
        )
    return None
