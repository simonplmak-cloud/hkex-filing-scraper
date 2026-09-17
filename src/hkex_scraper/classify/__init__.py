"""Filing classification package (A-003/A-004/A-005).

Deterministic-first, EN/ZH keyword classifiers for valuation, internal-control,
and sustainability (ESG) triggers, feeding the opportunity pipeline.
"""

from .esg_rules import ESGClassification, classify_scope_gap, classify_sustainability
from .ic_rules import ICClassification, classify_internal_control
from .valuation_rules import TRIGGERS, ValuationClassification, classify_valuation

__all__ = [
    "ESGClassification",
    "ICClassification",
    "TRIGGERS",
    "ValuationClassification",
    "classify_internal_control",
    "classify_scope_gap",
    "classify_sustainability",
    "classify_valuation",
]
