"""Deterministic valuation-trigger classification (A-003).

Classifies a filing title/description against the five valuation triggers
(very substantial acquisition/disposal, connected transaction, property
transaction, share-based payment, business combination) using EN/ZH keyword
sets. Overlapping flags are preserved (one filing may match several).
"""

from __future__ import annotations

from dataclasses import dataclass

from .confidence import highest


@dataclass(frozen=True)
class Trigger:
    id: str
    rule: str
    confidence: str
    keywords_en: tuple[str, ...]
    keywords_zh: tuple[str, ...]


@dataclass(frozen=True)
class ValuationClassification:
    valuation_required: bool
    confidence: str  # High | Medium | Low
    triggers: tuple[str, ...]
    reasons: tuple[str, ...]
    via: str  # "rules" | "model"


TRIGGERS: tuple[Trigger, ...] = (
    Trigger(
        "very_substantial_acquisition_disposal",
        "Listing Rule Ch.14",
        "High",
        (
            "VERY SUBSTANTIAL ACQUISITION",
            "VERY SUBSTANTIAL DISPOSAL",
            "VERY SUBSTANTIAL TRANSACTION",
        ),
        ("非常重大收购", "非常重大出售", "非常重大交易"),
    ),
    Trigger(
        "connected_transaction",
        "Listing Rule Ch.14A",
        "Medium",
        ("CONNECTED TRANSACTION", "CONTINUING CONNECTED TRANSACTION"),
        ("关连交易", "持续关连交易"),
    ),
    Trigger(
        "property_transaction",
        "Listing Rule Ch.5 / PN12",
        "Medium",
        (
            "PROPERTY TRANSACTION",
            "ACQUISITION OF PROPERTY",
            "DISPOSAL OF PROPERTY",
            "PROPERTY ACQUISITION",
            "PROPERTY DISPOSAL",
        ),
        ("物业交易", "收购物业", "出售物业", "物业收购", "物业出售"),
    ),
    Trigger(
        "share_based_payment",
        "HKFRS 2",
        "High",
        (
            "SHARE OPTION SCHEME",
            "SHARE AWARD SCHEME",
            "SHARE-BASED PAYMENT",
            "GRANT OF SHARE OPTIONS",
            "SHARE INCENTIVE SCHEME",
            "RESTRICTED SHARE",
        ),
        ("购股权计划", "股份奖励计划", "股份支付", "授出购股权", "股份激励计划"),
    ),
    Trigger(
        "business_combination",
        "HKFRS 3 / HKFRS 13 / HKAS 40",
        "Medium",
        ("BUSINESS COMBINATION", "MERGER", "AMALGAMATION", "ACQUISITION", "DISPOSAL"),
        ("业务合并", "合并", "兼并", "收购", "出售"),
    ),
)

NON_TRIGGER_KEYWORDS_EN = (
    "ANNUAL RESULTS",
    "ANNUAL REPORT",
    "INTERIM RESULTS",
    "INTERIM REPORT",
    "DIRECTOR APPOINTMENT",
    "APPOINTMENT OF DIRECTOR",
)

NON_TRIGGER_KEYWORDS_ZH = (
    "年度业绩",
    "年度报告",
    "中期业绩",
    "中期报告",
    "委任董事",
    "董事委任",
)


def _matches(trigger: Trigger, text: str, text_upper: str) -> bool:
    if any(k in text_upper for k in trigger.keywords_en):
        return True
    return any(k in text for k in trigger.keywords_zh)


def classify_valuation(title: str, description: str = "") -> ValuationClassification:
    text = f"{title or ''} {description or ''}".strip()
    if not text:
        return ValuationClassification(False, "Low", (), ("insufficient text",), "rules")

    text_upper = text.upper()
    matched = [t for t in TRIGGERS if _matches(t, text, text_upper)]
    if matched:
        confidence = highest(t.confidence for t in matched)
        return ValuationClassification(
            True,
            confidence,
            tuple(t.id for t in matched),
            tuple(f"{t.id} ({t.rule})" for t in matched),
            "rules",
        )

    if any(k in text_upper for k in NON_TRIGGER_KEYWORDS_EN) or any(
        k in text for k in NON_TRIGGER_KEYWORDS_ZH
    ):
        return ValuationClassification(False, "High", (), ("non-trigger filing",), "rules")

    # No rule decides — hand to the model fallback (default: no model wired).
    from .valuation_model import classify_undecidable

    return classify_undecidable(text)
