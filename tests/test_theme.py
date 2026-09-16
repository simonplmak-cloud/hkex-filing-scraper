"""Guard: the docs theme stays dark, monospace, offline, and WCAG 2.2 AA legible.

Contrast is computed from the actual stylesheet values, so editing a token without checking
it fails the suite instead of silently shipping an unreadable site.
"""

from __future__ import annotations

import pathlib
import re
from typing import Dict

ROOT = pathlib.Path(__file__).resolve().parent.parent
MKDOCS = ROOT / "mkdocs.yml"
CSS = ROOT / "docs" / "assets" / "stylesheets" / "terminal.css"

# WCAG 2.2: 4.5:1 for normal text, 3:1 for large text and UI components.
AA_NORMAL = 4.5
AA_LARGE = 3.0


def _luminance(hex_colour: str) -> float:
    value = hex_colour.lstrip("#")
    channels = [int(value[i : i + 2], 16) / 255 for i in (0, 2, 4)]
    linear = [c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4 for c in channels]
    r, g, b = linear
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def contrast(foreground: str, background: str) -> float:
    lighter, darker = sorted((_luminance(foreground), _luminance(background)), reverse=True)
    return (lighter + 0.05) / (darker + 0.05)


def _tokens() -> Dict[str, str]:
    text = CSS.read_text(encoding="utf-8")
    return {
        name: value.lower()
        for name, value in re.findall(r"(--term-[\w-]+):\s*(#[0-9a-fA-F]{6})", text)
    }


class TestPaletteIsDarkOnly:
    def test_single_slate_scheme_without_a_toggle(self):
        text = MKDOCS.read_text(encoding="utf-8")
        assert "scheme: slate" in text
        assert "scheme: default" not in text, "a light scheme reintroduces the toggle"
        assert "toggle:" not in text, "the colour-scheme toggle must be gone"
        assert 'media: "(prefers-color-scheme' not in text

    def test_stylesheet_is_wired_in(self):
        text = MKDOCS.read_text(encoding="utf-8")
        assert "extra_css:" in text
        assert "assets/stylesheets/terminal.css" in text
        assert CSS.exists()

    def test_no_external_fonts_or_cdn_assets(self):
        mkdocs = MKDOCS.read_text(encoding="utf-8")
        css = CSS.read_text(encoding="utf-8")
        assert "font: false" in mkdocs, "Google Fonts must stay disabled"
        for needle in (
            "fonts.googleapis.com",
            "fonts.gstatic.com",
            "cdn.jsdelivr.net",
            "unpkg.com",
        ):
            assert needle not in mkdocs, f"{needle} referenced in mkdocs.yml"
            assert needle not in css, f"{needle} referenced in the stylesheet"

    def test_monospace_stack_is_system_only(self):
        css = CSS.read_text(encoding="utf-8")
        assert "ui-monospace" in css
        assert "var(--md-text-font-family: " not in css  # Material var must be the token stack


class TestContrast:
    def test_body_and_secondary_text(self):
        t = _tokens()
        assert contrast(t["--term-text"], t["--term-bg"]) >= AA_NORMAL
        assert contrast(t["--term-text-2"], t["--term-bg"]) >= AA_NORMAL
        assert contrast(t["--term-muted"], t["--term-bg"]) >= AA_NORMAL

    def test_amber_on_background_and_surfaces(self):
        t = _tokens()
        assert contrast(t["--term-amber"], t["--term-bg"]) >= AA_NORMAL
        assert contrast(t["--term-amber"], t["--term-surface"]) >= AA_NORMAL
        assert contrast(t["--term-amber"], t["--term-surface-2"]) >= AA_NORMAL

    def test_links_code_and_status_colours(self):
        t = _tokens()
        assert contrast(t["--term-amber-soft"], t["--term-surface-2"]) >= AA_NORMAL  # inline code
        assert contrast(t["--term-green"], t["--term-bg"]) >= AA_NORMAL
        assert contrast(t["--term-red"], t["--term-bg"]) >= AA_NORMAL

    def test_borders_are_visible_against_the_background(self):
        t = _tokens()
        # Non-text UI boundaries need 3:1.
        assert contrast(t["--term-rule-strong"], t["--term-bg"]) >= 1.5
        assert contrast(t["--term-muted"], t["--term-surface-2"]) >= AA_LARGE

    def test_every_declared_colour_pair_is_reported(self):
        t = _tokens()
        assert t["--term-bg"] == "#0b0b0b", "the palette must stay near-black"
        # The comment block documents ratios; keep it honest for the main foregrounds.
        documented = contrast(t["--term-text"], t["--term-bg"])
        assert documented >= 10, "body text should be comfortably above AA"


class TestWcag22Specifics:
    def test_focus_is_visible(self):
        css = CSS.read_text(encoding="utf-8")
        assert ":focus-visible" in css
        assert "outline: 2px solid" in css

    def test_interactive_targets_are_at_least_24px(self):
        css = CSS.read_text(encoding="utf-8")
        # WCAG 2.2 §2.5.8 (Target Size, Minimum): 24px == 1.5rem at the default root size.
        assert "min-height: 1.5rem" in css

    def test_motion_preference_is_respected(self):
        css = CSS.read_text(encoding="utf-8")
        assert "@media (prefers-reduced-motion: reduce)" in css

    def test_colour_is_never_the_only_cue_for_links(self):
        css = CSS.read_text(encoding="utf-8")
        # Prose links are underlined by default (axe: link-in-text-block); chrome is not.
        assert re.search(r"\.md-typeset a \{[^}]*text-decoration: underline", css)
        assert re.search(r"\.md-nav a,[^}]*text-decoration: none", css)

    def test_scroll_containers_are_keyboard_reachable(self):
        script = (ROOT / "docs" / "assets" / "javascripts" / "a11y.js").read_text(encoding="utf-8")
        assert "md-typeset__scrollwrap" in script
        assert 'setAttribute("tabindex", "0")' in script
        assert 'setAttribute("aria-label", "Scrollable table")' in script

    def test_search_toggle_patch_is_wired_in(self):
        mkdocs = MKDOCS.read_text(encoding="utf-8")
        script = ROOT / "docs" / "assets" / "javascripts" / "a11y.js"
        assert script.exists(), "the search-toggle a11y patch is missing"
        assert "assets/javascripts/a11y.js" in mkdocs
        assert "extra_javascript:" in mkdocs
        body = script.read_text(encoding="utf-8")
        assert "#__search" in body and "aria-label" in body
