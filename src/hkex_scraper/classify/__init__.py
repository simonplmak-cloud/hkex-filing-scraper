"""Valuation-trigger classification (A-003).

Deterministic-first classifier for the five valuation triggers with EN/ZH
keyword sets, a confidence tier, and the triggering rule. A filing no rule can
decide is handed to the (default no-model) fallback — never silently "not
required".
"""

from .valuation_rules import TRIGGERS, ValuationClassification, classify_valuation

__all__ = ["TRIGGERS", "ValuationClassification", "classify_valuation"]
