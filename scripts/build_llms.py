#!/usr/bin/env python3
"""Build ``llms-full.txt`` — the full docs content in one file for LLM consumption.

The `llmstxt.org` convention ships two files: a curated ``llms.txt`` (hand-maintained in
``docs/llms.txt``) and ``llms-full.txt`` (the complete content). This script concatenates
every Markdown source under ``docs/`` in deterministic (sorted) order, so the file stays
complete with no maintenance when pages are added.

Usage (run after ``mkdocs build`` so the output lands in the site dir):

    python scripts/build_llms.py [OUTPUT]   # default: site/llms-full.txt
"""

from __future__ import annotations

import sys
from pathlib import Path

DOCS_DIR = Path("docs")
BASE_URL = "https://hkex-listco-updates.ascent-partners.com/"


def _page_url(path: Path) -> str:
    rel = path.relative_to(DOCS_DIR).with_suffix("").as_posix()
    if rel == "index":
        return BASE_URL
    return f"{BASE_URL}{rel}/"


def main() -> int:
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("site/llms-full.txt")
    sources = sorted(DOCS_DIR.rglob("*.md"))

    parts = ["# HKEx Filing Scraper — full documentation\n"]
    parts.append(
        "This file concatenates the complete documentation site so an LLM can read it in a "
        "single pass. See the curated index at "
        f"{BASE_URL}llms.txt.\n"
    )

    for src in sources:
        text = src.read_text(encoding="utf-8")
        title = next(
            (line[2:].strip() for line in text.splitlines() if line.startswith("# ")),
            src.stem,
        )
        parts.append("\n\n---\n\n")
        parts.append(f"## {title}\n")
        parts.append(f"<!-- source: {_page_url(src)} -->\n")
        parts.append(text)

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("".join(parts), encoding="utf-8")
    print(f"Wrote {out} from {len(sources)} pages.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
