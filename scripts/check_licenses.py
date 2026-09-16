#!/usr/bin/env python3
"""Fail if the current environment contains a copyleft dependency.

Run with the interpreter of the environment to check (for example a throwaway venv where
``pip install ".[all]"`` was run). Uses only the standard library, so it needs no extra tool.

Strong copyleft (GPL/AGPL) is rejected; the weak copyleft LGPL is accepted because it does not
propagate to this project's MIT code. The AGPL-3.0 document-extraction stack is deliberately
kept out of the ``all`` extra and is therefore not installed here.

Usage::

    python scripts/check_licenses.py            # exit 1 on a copyleft dependency
    python scripts/check_licenses.py --verbose  # print every dependency and its licence
"""

from __future__ import annotations

import argparse
import re
import sys
from importlib import metadata

# (?!L)GPL matches GPL and AGPL but not LGPL.
COPYLEFT = re.compile(r"(?<!L)(?:A?GPL)", re.IGNORECASE)


def licence_text(dist: metadata.Distribution) -> str:
    """Collect the licence signals a distribution exposes."""
    meta = dist.metadata
    parts = [
        str(meta.get("License-Expression") or ""),
        str(meta.get("License") or ""),
        " ".join(meta.get_all("Classifier") or []),
    ]
    return " ".join(parts)


def offenders() -> list[tuple[str, str, str]]:
    """Return (name, version, licence text) for every copyleft dependency."""
    found = []
    for dist in metadata.distributions():
        text = licence_text(dist)
        if COPYLEFT.search(text):
            name = str(dist.metadata.get("Name") or "?")
            version = str(dist.version or "?")
            found.append((name, version, " ".join(text.split())))
    return sorted(found)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verbose", action="store_true", help="list every dependency")
    args = parser.parse_args(argv)

    if args.verbose:
        for dist in sorted(metadata.distributions(), key=lambda d: str(d.metadata.get("Name"))):
            text = licence_text(dist) or "(no licence metadata)"
            print(f"{dist.metadata.get('Name')} {dist.version}: {' '.join(text.split())[:90]}")

    found = offenders()
    if found:
        print("Copyleft dependencies found:", file=sys.stderr)
        for name, version, text in found:
            print(f"  - {name} {version}: {text}", file=sys.stderr)
        print(
            "Move them into an explicitly labelled extra, or pick a permissive alternative.",
            file=sys.stderr,
        )
        return 1

    print("No copyleft dependencies.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
