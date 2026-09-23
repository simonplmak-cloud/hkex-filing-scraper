# HKEx filings — read-only MCP server over stdio.
#
# Used by Glama (and other registries) for reproducible sandboxed introspection.
# The version is derived from git tags via hatch-vcs (setuptools-scm), but .git is
# excluded from the build context, so SETUPTOOLS_SCM_PRETEND_VERSION pins it.
# Bump it alongside each release.

FROM python:3.12-slim

WORKDIR /app

ARG HATCH_VERSION=2.4.0
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${HATCH_VERSION} \
    PIP_NO_CACHE_DIR=1

COPY pyproject.toml README.md LICENSE ./
COPY src ./src

RUN pip install ".[mcp]"

# stdio MCP server — stdout carries the protocol; keep it clean.
CMD ["hkex-scraper-mcp"]
