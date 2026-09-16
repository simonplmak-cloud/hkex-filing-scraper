"""Guard: supply-chain and packaging hygiene.

These assertions keep the controls added in the supply-chain hardening pass from
silently regressing: pinned Actions, least-privilege workflow tokens, digest-pinned
images, the permissive `all` extra, and PEP 639 licence metadata.
"""

from __future__ import annotations

import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent
WORKFLOWS = ROOT / ".github" / "workflows"

PINNED_ACTION = re.compile(r"^\s*(?:- )?uses: [^@\s]+@[0-9a-f]{40}(?: # \S+)?$")


def _workflow_files() -> list[pathlib.Path]:
    return sorted(WORKFLOWS.glob("*.yml"))


class TestActionPinning:
    def test_there_are_workflows(self):
        assert _workflow_files(), "no workflow files found"

    def test_every_action_is_pinned_to_a_commit_sha(self):
        for path in _workflow_files():
            for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if "uses:" not in line:
                    continue
                if line.lstrip().startswith("#"):
                    continue
                assert PINNED_ACTION.match(line), (
                    f"{path.name}:{lineno}: action is not pinned to a 40-char SHA: {line.strip()}"
                )

    def test_every_workflow_declares_permissions(self):
        for path in _workflow_files():
            text = path.read_text(encoding="utf-8")
            assert re.search(r"(?m)^permissions:", text), (
                f"{path.name} does not declare top-level permissions"
            )

    def test_default_permission_is_read_only(self):
        for path in _workflow_files():
            text = path.read_text(encoding="utf-8")
            block = text.split("permissions:", 1)[1].split("jobs:", 1)[0]
            assert "write-all" not in block, f"{path.name} grants write-all"
            assert "write" not in block, (
                f"{path.name}: the top-level permissions must be read-only; grant writes "
                "per job instead"
            )

    ENGINE_IMAGES = (
        "postgres:",
        "mysql:",
        "mariadb:",
        "mongo:",
        "clickhouse/clickhouse-server:",
        "neo4j:",
        "surrealdb/surrealdb:",
    )

    def _unpinned_images(self, text: str) -> list[str]:
        """Container image references that are missing an @sha256 digest."""
        prefixes = "|".join(re.escape(p) for p in self.ENGINE_IMAGES)
        pattern = re.compile(rf"(?:{prefixes})[\w.-]+(@sha256:[0-9a-f]{{64}})?")
        return [m.group(0) for m in pattern.finditer(text) if not m.group(1)]

    def test_images_are_pinned_by_digest(self):
        targets = [*(WORKFLOWS.glob("*.yml")), ROOT / "examples" / "docker-compose.yml"]
        for path in targets:
            text = path.read_text(encoding="utf-8")
            assert not self._unpinned_images(text), (
                f"{path.name}: unpinned container image(s): {self._unpinned_images(text)}"
            )

    def test_service_images_use_the_image_key(self):
        # `services:` images must be declared with `image:` and a digest.
        for path in WORKFLOWS.glob("*.yml"):
            for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if line.strip().startswith("image:"):
                    assert "@sha256:" in line, (
                        f"{path.name}:{lineno}: image is not pinned by digest: {line.strip()}"
                    )


class TestPackagingMetadata:
    def test_pep639_license_expression(self):
        text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
        assert re.search(r'(?m)^license = "MIT"$', text), "license must be a PEP 639 SPDX string"
        assert 'license-files = ["LICENSE"]' in text, "license-files must list LICENSE"
        assert "license = { text" not in text, "the pre-PEP 639 license table is gone"

    def test_hatchling_supports_pep639(self):
        text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
        assert re.search(r'"hatchling>=1\.26"', text), "hatchling>=1.26 emits PEP 639 metadata"

    def test_all_extra_excludes_the_agpl_pdf_stack(self):
        text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
        block = text.split("[project.optional-dependencies]", 1)[1].split("\n[", 1)[0]
        all_extra = block.split("all = [", 1)[1].split("]", 1)[0].lower()
        for agpl in ("pymupdf", "pymupdf4llm", "camelot"):
            assert agpl not in all_extra, f"`all` must not pull the AGPL {agpl} dependency"
        pdf_extra = block.split("pdf = [", 1)[1].split("]", 1)[0].lower()
        assert "pymupdf" in pdf_extra, "the AGPL stack belongs to the explicit `pdf` extra"


class TestSecurityDocs:
    def test_rotation_runbook_exists(self):
        text = (ROOT / "SECURITY.md").read_text(encoding="utf-8")
        assert "## Credential rotation" in text
        assert "gh secret set WIKI_TOKEN" in text

    def test_release_verification_is_documented(self):
        text = (ROOT / "docs" / "releasing.md").read_text(encoding="utf-8")
        assert "gh attestation verify" in text


class TestLicenseGate:
    def test_copyleft_pattern_matches_gpl_and_agpl_but_not_lgpl(self):
        import importlib.util

        path = ROOT / "scripts" / "check_licenses.py"
        spec = importlib.util.spec_from_file_location("check_licenses", path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        assert module.COPYLEFT.search("GNU General Public License v2 (GPLv2)")
        assert module.COPYLEFT.search("AGPL-3.0-only")
        assert not module.COPYLEFT.search("GNU Lesser General Public License v3 (LGPLv3)")
        assert not module.COPYLEFT.search("MIT License")

    def test_ci_installs_the_all_extra_for_the_license_check(self):
        text = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        assert "scripts/check_licenses.py" in text


class TestSupplyChainJobCoverage:
    """Every runtime extra must be audited, and the AGPL/dev extras must not be."""

    # Extras intentionally outside the audit: the AGPL stack, developer tooling, and the
    # `all` meta-extra (whose members are exported explicitly).
    UNAUDITED = {"pdf", "dev", "all"}

    def test_every_runtime_extra_is_exported_for_audit(self):
        import re as _re

        pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
        block = pyproject.split("[project.optional-dependencies]", 1)[1].split("\n[", 1)[0]
        extras = set(_re.findall(r"(?m)^([A-Za-z0-9_-]+) = \[", block))
        runtime = extras - self.UNAUDITED

        ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        step = ci.split("Export the locked runtime dependency set", 1)[1].split("- name:", 1)[0]
        for extra in sorted(runtime):
            assert f"--extra {extra}" in step, (
                f"runtime extra `{extra}` is not audited by the supply-chain job"
            )

    def test_agpl_and_dev_extras_are_not_audited(self):
        ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        step = ci.split("Export the locked runtime dependency set", 1)[1].split("- name:", 1)[0]
        assert "--extra pdf" not in step
        assert "--extra dev" not in step
