"""Model fallback for valuation classification (A-003).

Deterministic rules are the primary path. A filing no rule can decide is handed
here; by default no model is wired, so it is reported as Low-confidence
"undecidable — model unavailable" (never silently "not required").
"""

from __future__ import annotations

from .valuation_rules import ValuationClassification


def classify_undecidable(text: str) -> ValuationClassification:
    del text  # reserved for a future language-model hook (would set via="model")
    return ValuationClassification(
        False,
        "Low",
        (),
        ("undecidable — model unavailable",),
        "rules",
    )
