# HKEx filings — read-only MCP server over stdio.
#
# Used by Glama (and the Docker MCP Registry) for reproducible sandboxed
# introspection: build this image, run it, and the server answers tools/list over
# stdio. The package version is derived from git tags via hatch-vcs, but .git is
# excluded from the build context, so it is pinned here. Bump alongside each release.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    SETUPTOOLS_SCM_PRETEND_VERSION=2.4.0

WORKDIR /app

COPY pyproject.toml README.md LICENSE ./
COPY src ./src

RUN pip install ".[mcp]"

# stdio MCP server — stdout carries the protocol; keep it clean.
ENTRYPOINT ["hkex-scraper-mcp"]
