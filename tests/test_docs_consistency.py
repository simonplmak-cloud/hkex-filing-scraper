"""Guard: user-facing docs and config must agree with the code.

This is the control for risk R4 (docs ↔ code drift). It fails a PR when a sink is
added, removed, or renamed without updating the docs, templates, or the env
template.
"""

from __future__ import annotations

import pathlib
import re
import tomllib

import pytest

from hkex_scraper import sinks

ROOT = pathlib.Path(__file__).resolve().parent.parent


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


# Sinks whose primary guide lives outside docs/backends/.
GUIDE_OVERRIDES = {
    "postgres": "docs/postgresql.md",
    "surrealdb": "docs/architecture.md",
}


class TestSinkIdsInDocs:
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
            rel = GUIDE_OVERRIDES.get(sink_id, f"docs/backends/{sink_id}.md")
            assert (ROOT / rel).exists(), f"missing guide for `{sink_id}` (looked for {rel})"

    def test_matrix_lists_every_sink(self):
        matrix = _read("docs/backends/README.md")
        for sink_id in sinks.known_ids():
            assert f"`{sink_id}`" in matrix, f"matrix does not list `{sink_id}`"


class TestExtrasAlignment:
    def _pyproject_extras(self) -> set[str]:
        with open(ROOT / "pyproject.toml", "rb") as handle:
            data = tomllib.load(handle)
        return set(data["project"]["optional-dependencies"])

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
