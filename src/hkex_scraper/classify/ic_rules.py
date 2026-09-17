"""Internal-control trigger classification (A-004).

Classifies a filing title/description against internal-control signals with a
severity ladder (Critical/High/Medium) and an aggregation rule: any critical
signal, or two or more medium signals, escalates to a priority account.
"""

from __future__ import annotations

from dataclasses import dataclass

SEVERITY_ORDER: dict[str, int] = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}


@dataclass(frozen=True)
class ICSignal:
    id: str
    severity: str  # Critical | High | Medium
    keywords_en: tuple[str, ...]
    keywords_zh: tuple[str, ...]


@dataclass(frozen=True)
class ICClassification:
    ic_required: bool
    severity: str  # Critical | High | Medium | Low
    signals: tuple[str, ...]
    escalated: bool
    reasons: tuple[str, ...]
    via: str


IC_SIGNALS: tuple[ICSignal, ...] = (
    ICSignal(
        "qualified_opinion",
        "Critical",
        ("QUALIFIED OPINION", "DISCLAIMER OF OPINION", "DISCLAIMED OPINION", "ADVERSE OPINION"),
        ("保留意见", "无法表示意见", "否定意见"),
    ),
    ICSignal(
        "auditor_resignation",
        "Critical",
        (
            "RESIGNATION OF AUDITOR",
            "AUDITOR RESIGNATION",
            "CHANGE OF AUDITOR",
            "CESSATION OF AUDITOR",
        ),
        ("核数师辞任", "辞任核数师", "核数师变更", "更换核数师"),
    ),
    ICSignal(
        "late_results_suspension",
        "Critical",
        (
            "DELAY IN PUBLICATION OF RESULTS",
            "SUSPENSION OF TRADING",
            "TRADING SUSPENSION",
            "DELISTING",
        ),
        ("延迟刊发业绩", "暂停买卖", "停牌", "除牌"),
    ),
    ICSignal(
        "material_ic_weakness",
        "High",
        (
            "MATERIAL INTERNAL CONTROL WEAKNESS",
            "INTERNAL CONTROL DEFICIENCY",
            "SIGNIFICANT DEFICIENCY",
        ),
        ("重大内部监控弱点", "内部监控缺陷", "内部监控重大缺失"),
    ),
    ICSignal(
        "restatement",
        "High",
        ("FINANCIAL RESTATEMENT", "RESTATEMENT OF FINANCIAL STATEMENTS"),
        ("财务重列", "重列财务"),
    ),
    ICSignal(
        "sfc_action",
        "High",
        (
            "SFC ENFORCEMENT",
            "SFC DISCIPLINARY",
            "DISCIPLINARY ACTION BY THE SFC",
            "REGULATORY ACTION",
        ),
        ("证监会执法", "证监会纪律行动", "证监会监管行动"),
    ),
    ICSignal(
        "cg_code_deficiency",
        "Medium",
        ("CORPORATE GOVERNANCE CODE", "NON-COMPLIANCE WITH THE CORPORATE GOVERNANCE CODE"),
        ("企业管治守则", "偏离企业管治守则"),
    ),
    ICSignal(
        "new_listing",
        "Medium",
        ("NEW LISTING", "INITIAL PUBLIC OFFERING", "IPO"),
        ("新上市", "首次公开招股"),
    ),
    ICSignal(
        "pn21",
        "Medium",
        ("PN21", "PRACTICE NOTE 21", "INTERNAL CONTROL REVIEW"),
        ("内部监控审阅", "内部监控审查"),
    ),
    ICSignal(
        "personnel_change",
        "Medium",
        (
            "RESIGNATION OF CHIEF FINANCIAL OFFICER",
            "RESIGNATION OF CFO",
            "CHANGE OF CHIEF FINANCIAL OFFICER",
            "RESIGNATION OF COMPANY SECRETARY",
            "CHANGE OF AUDIT COMMITTEE",
        ),
        ("辞任首席财务官", "变更首席财务官", "委任首席财务官", "辞任公司秘书", "变更审核委员会"),
    ),
)


def _matches(signal: ICSignal, text: str, text_upper: str) -> bool:
    if any(k in text_upper for k in signal.keywords_en):
        return True
    return any(k in text for k in signal.keywords_zh)


def classify_internal_control(title: str, description: str = "") -> ICClassification:
    text = f"{title or ''} {description or ''}".strip()
    if not text:
        return ICClassification(False, "Low", (), False, ("insufficient text",), "rules")

    text_upper = text.upper()
    matched = [s for s in IC_SIGNALS if _matches(s, text, text_upper)]
    if not matched:
        return ICClassification(False, "Low", (), False, ("no internal-control signal",), "rules")

    severity = min(matched, key=lambda s: SEVERITY_ORDER[s.severity]).severity
    critical = any(s.severity == "Critical" for s in matched)
    medium_count = sum(1 for s in matched if s.severity == "Medium")
    escalated = critical or medium_count >= 2

    return ICClassification(
        True,
        severity,
        tuple(s.id for s in matched),
        escalated,
        tuple(f"{s.id} ({s.severity})" for s in matched),
        "rules",
    )
