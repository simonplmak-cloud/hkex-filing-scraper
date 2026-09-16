"""Guard: user-facing docs and config must agree with the code.

This is the control for risk R4 (docs ↔ code drift). It fails a PR when a sink is
added, removed, or renamed without updating the docs, templates, or the env
template.
"""

from __future__ import annotations

import pathlib
import re

import pytest

from hkex_scraper import sinks

ROOT = pathlib.Path(__file__).resolve().parent.parent


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


# Sinks whose primary guide lives elsewhere (shared or top-level).
GUIDE_OVERRIDES = {
    "postgres": "docs/sinks/postgresql.md",  # page slug matches the docs URL
    "mariadb": "docs/sinks/mysql.md",  # mysql.md documents both engines
}

# The documented order: sinks sorted by developer usage (Stack Overflow Developer Survey
# 2025), then the remaining engines by DB-Engines popularity. Kept here as a literal so that
# adding, removing, or renaming a sink fails until the docs and this list are updated together.
# It must match `sinks/registry.py:POPULARITY_ORDER`.
POPULARITY_ORDER = [
    "postgres",
    "mysql",
    "sqlite",
    "mongodb",
    "mariadb",
    "neo4j",
    "clickhouse",
    "duckdb",
    "surrealdb",
]
DOCUMENTED_ID_LIST = ", ".join(f"`{sid}`" for sid in POPULARITY_ORDER)

# Files that must list every sink id, in order, on the DATABASE_TARGET reference row.
CANONICAL_LIST_FILES = (
    "README.md",
    "docs/configuration.md",
    "docs/cli.md",
)


class TestSinkIdsInDocs:
    def test_documented_order_matches_the_registry(self):
        assert POPULARITY_ORDER == sinks.known_ids(), (
            "POPULARITY_ORDER is out of step with sinks/registry.py:POPULARITY_ORDER; "
            "update the registry, the docs reference rows, and this list together"
        )

    def test_canonical_id_list_is_literal_in_reference_rows(self):
        for rel in CANONICAL_LIST_FILES:
            assert DOCUMENTED_ID_LIST in _read(rel), (
                f"{rel} must list every sink id in documented order on the DATABASE_TARGET row"
            )

    def test_readme_lists_every_sink(self):
        readme = _read("README.md")
        for sink_id in sinks.known_ids():
            assert f"`{sink_id}`" in readme, f"README does not mention sink `{sink_id}`"

    def test_cli_doc_lists_every_sink(self):
        cli = _read("docs/cli.md")
        for sink_id in sinks.known_ids():
            assert f"`{sink_id}`" in cli, f"docs/cli.md does not mention sink `{sink_id}`"

    def test_bug_template_lists_every_sink(self):
        template = _read(".github/ISSUE_TEMPLATE/bug_report.yml")
        for sink_id in sinks.known_ids():
            assert f"- {sink_id}" in template, f"bug_report.yml missing option {sink_id}"

    def test_configuration_doc_lists_every_sink(self):
        configuration = _read("docs/configuration.md")
        for sink_id in sinks.known_ids():
            assert f"`{sink_id}`" in configuration, f"configuration.md missing {sink_id}"

    def test_no_stale_both_or_default_wording(self):
        for rel in ("docs/configuration.md", "docs/cli.md", "README.md"):
            text = _read(rel)
            assert not re.search(r"DATABASE_TARGET=both", text), f"{rel} still documents 'both'"
            assert "surrealdb (default)" not in text, f"{rel} still calls surrealdb the default"


class TestBackendGuides:
    def test_every_sink_has_a_guide(self):
        for sink_id in sinks.known_ids():
            rel = GUIDE_OVERRIDES.get(sink_id, f"docs/sinks/{sink_id}.md")
            assert (ROOT / rel).exists(), f"missing guide for `{sink_id}` (looked for {rel})"

    def test_matrix_lists_every_sink(self):
        matrix = _read("docs/sinks/README.md")
        for sink_id in sinks.known_ids():
            assert f"`{sink_id}`" in matrix, f"matrix does not list `{sink_id}`"


class TestExtrasAlignment:
    def _pyproject_extras(self) -> set[str]:
        # Parse the table text rather than tomllib (stdlib only from Python 3.11).
        text = _read("pyproject.toml")
        section = text.split("[project.optional-dependencies]", 1)[1].split("\n[", 1)[0]
        extras = set()
        for line in section.splitlines():
            match = re.match(r"^([A-Za-z0-9_-]+)\s*=", line)
            if match:
                extras.add(match.group(1))
        return extras

    def test_every_extra_is_documented_in_readme(self):
        readme = _read("README.md")
        for extra in self._pyproject_extras():
            assert f"`{extra}`" in readme, f"README does not document extra `{extra}`"

    def test_every_sink_extra_exists_in_pyproject(self):
        extras = self._pyproject_extras()
        for sink_id, spec in sinks.SINKS.items():
            if spec.extra is not None:
                assert spec.extra in extras, (
                    f"sink `{sink_id}` declares extra `{spec.extra}` not present in pyproject"
                )


class TestEnvTemplate:
    def test_env_example_exists_and_covers_sinks(self):
        path = ROOT / ".env.example"
        if not path.exists():
            pytest.skip(".env.example not present")
        text = path.read_text(encoding="utf-8")
        for sink_id in sinks.known_ids():
            assert f"`{sink_id}`" in text or sink_id in text, (
                f".env.example does not mention sink `{sink_id}`"
            )
        assert "both" not in text, ".env.example still documents the removed `both` alias"
        assert "DATABASE_TARGET=surrealdb" not in text, ".env.example still sets a silent default"


def _wiki_module():
    import importlib.util

    path = ROOT / "scripts/mirror_wiki.py"
    spec = importlib.util.spec_from_file_location("mirror_wiki", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWikiMirror:
    """The wiki mirror must stay complete as docs are added or removed."""

    def test_every_doc_has_a_wiki_page(self):
        module = _wiki_module()
        assert module.missing_from_map(ROOT / "docs") == [], (
            "a doc under docs/ is not mapped in scripts/mirror_wiki.py:SECTIONS"
        )

    def test_wiki_page_map_has_no_dangling_entries(self):
        module = _wiki_module()
        assert module.extra_in_map(ROOT / "docs") == [], (
            "scripts/mirror_wiki.py maps a doc that no longer exists"
        )

    def test_sidebar_is_generated(self):
        module = _wiki_module()
        sidebar = module.render_sidebar()
        for page in module.PAGE_MAP.values():
            assert f"[[{page}]]" in sidebar, f"sidebar is missing {page}"


class TestDocumentationStyle:
    """Guard the house style: US English and one canonical term (docs/STYLE.md)."""

    STYLE_FILES = (
        "README.md",
        "CONTRIBUTING.md",
        "SECURITY.md",
        "SUPPORT.md",
        "GOVERNANCE.md",
        "CODE_OF_CONDUCT.md",
        "docs/index.md",
        "docs/getting-started.md",
        "docs/configuration.md",
        "docs/cli.md",
        "docs/testing.md",
        "docs/troubleshooting.md",
        "docs/upgrading.md",
        "docs/sinks/postgresql.md",
        "docs/sinks/surrealdb.md",
        "docs/sinks/README.md",
    )

    def test_style_guide_exists(self):
        assert (ROOT / "docs/STYLE.md").exists(), "docs/STYLE.md is missing"

    def test_us_spelling(self):
        offenders = [rel for rel in self.STYLE_FILES if "licence" in _read(rel)]
        assert not offenders, f"UK spelling 'licence' found in: {', '.join(offenders)}"

    def test_no_backend_synonym_in_prose(self):
        # The canonical term is "sink"; "backend" survives only in the build-backend
        # sense (hatchling), which does not appear in these files.
        offenders = [rel for rel in self.STYLE_FILES if "backend" in _read(rel).lower()]
        assert not offenders, f"use 'sink' instead of 'backend' in: {', '.join(offenders)}"
