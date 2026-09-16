"""HKEx Filing Scraper — scrape HKEx regulatory filings into SurrealDB and/or PostgreSQL."""

__version__: str
try:
    from ._version import __version__
except ImportError:  # pragma: no cover - source checkout before a build
    # hatch-vcs generates _version.py at build/install time from the git tag.
    __version__ = "0.0.0+unknown"

__all__ = ["__version__"]
