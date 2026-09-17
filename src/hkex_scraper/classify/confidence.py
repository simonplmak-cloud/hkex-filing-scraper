"""Confidence ordering for classifications (High > Medium > Low)."""

from __future__ import annotations

from typing import Iterable

CONFIDENCE_ORDER: dict[str, int] = {"High": 0, "Medium": 1, "Low": 2}


def highest(confidences: Iterable[str]) -> str:
    """Return the highest-confidence label in an iterable of labels."""
    return min(confidences, key=lambda c: CONFIDENCE_ORDER.get(c, 2))
