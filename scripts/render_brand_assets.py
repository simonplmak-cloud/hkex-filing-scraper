#!/usr/bin/env python3
"""Render the brand SVG sources to the committed PNG assets.

Chromium is used (a headless screenshot) rather than an SVG library: it resolves the
embedded CSS, gradients, and system monospace fonts exactly as a browser does, and a
Chromium build is already present on the build box (Playwright's bundled copy). The PNGs
are committed, so this only needs to run when an SVG changes.

Usage:

    python scripts/render_brand_assets.py            # render every target
    python scripts/render_brand_assets.py --check    # verify the committed PNG sizes
"""

from __future__ import annotations

import argparse
import glob
import os
import pathlib
import shutil
import struct
import subprocess

ROOT = pathlib.Path(__file__).resolve().parent.parent
ASSETS = ROOT / "docs" / "assets"

# (source SVG in docs/assets, committed PNG, width, height)
TARGETS = (
    ("social.svg", ROOT / "docs" / "social_preview.png", 1280, 640),
    ("mcp.svg", ASSETS / "mcp.png", 1120, 420),
)

_CHROME_GLOBS = (
    "/root/.cache/ms-playwright/chromium-*/chrome-linux64/chrome",
    str(pathlib.Path.home() / ".cache/ms-playwright/chromium-*/chrome-linux64/chrome"),
    "/usr/bin/google-chrome",
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
)


def find_chrome() -> str:
    """Locate a Chrome/Chromium executable (``CHROME_BIN`` wins)."""
    env = os.environ.get("CHROME_BIN")
    if env and pathlib.Path(env).is_file():
        return env
    for pattern in _CHROME_GLOBS:
        for match in sorted(glob.glob(pattern), reverse=True):
            if pathlib.Path(match).is_file():
                return match
    found = (
        shutil.which("google-chrome")
        or shutil.which("chromium")
        or shutil.which("chromium-browser")
    )
    if found:
        return found
    raise SystemExit("No Chromium found; set CHROME_BIN to a Chrome/Chromium executable.")


def png_size(path: pathlib.Path) -> tuple[int, int]:
    with path.open("rb") as handle:
        header = handle.read(24)
    if header[:8] != b"\x89PNG\r\n\x1a\n":
        raise SystemExit(f"{path} is not a PNG")
    width, height = struct.unpack(">II", header[16:24])
    return width, height


def render(chrome: str, svg: pathlib.Path, out: pathlib.Path, width: int, height: int) -> None:
    if not svg.exists():
        raise SystemExit(f"missing source SVG: {svg}")
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        out.unlink()
    subprocess.run(
        [
            chrome,
            "--headless=new",
            "--no-sandbox",
            "--disable-gpu",
            "--hide-scrollbars",
            "--force-device-scale-factor=1",
            f"--window-size={width},{height}",
            f"--screenshot={out}",
            svg.resolve().as_uri(),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    actual = png_size(out)
    if actual != (width, height):
        raise SystemExit(f"{out} rendered {actual[0]}x{actual[1]}, expected {width}x{height}")


def check() -> int:
    failures = 0
    for _name, out, width, height in TARGETS:
        if not out.exists():
            print(f"MISSING  {out.relative_to(ROOT)}")
            failures += 1
            continue
        actual = png_size(out)
        ok = actual == (width, height)
        print(f"{'ok      ' if ok else 'WRONG   '}{out.relative_to(ROOT)} {actual[0]}x{actual[1]}")
        failures += 0 if ok else 1
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="verify the committed PNG sizes")
    args = parser.parse_args(argv)
    if args.check:
        return check()
    chrome = find_chrome()
    for name, out, width, height in TARGETS:
        render(chrome, ASSETS / name, out, width, height)
        print(f"rendered {out.relative_to(ROOT)} ({width}x{height})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
