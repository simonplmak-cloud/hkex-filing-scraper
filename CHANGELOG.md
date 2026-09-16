# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/2.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.0] - 2026-09-16

### Added

- **PostgreSQL sink (dual-store).** `DATABASE_TARGET` selects `surrealdb` (default), `postgres`, or `both`. The scraper mirrors filing metadata, document payloads (`document_tables` as `jsonb`, `referenced_tickers` as `text[]`), coverage rows, and graph edges into PostgreSQL with idempotent `ON CONFLICT` upserts. Schema is created automatically with `CREATE TABLE IF NOT EXISTS`.
- `--database-target` CLI override and `--parity-report` (per-sink filing counts and difference).
- `postgres` optional extra (`psycopg[binary,pool]>=3.1`), included in `all`.
- Per-sink success/failure counters and a sink summary printed every run; non-zero exit when a required sink fails.
- Documentation: `docs/` (getting started, PostgreSQL guide, configuration, CLI, architecture, troubleshooting, upgrading), `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`, issue and PR templates, and this changelog.
- CI workflow (lint, format check, tests on Python 3.10–3.13, optional PostgreSQL integration job).

### Changed

- Composite `scrape_coverage` table added to the SurrealDB schema; `scrape_coverage` mirrored to PostgreSQL.
- Sink reads are routed by target: with `DATABASE_TARGET=postgres`, Phase 2 pending-filing selection and graph ticker enumeration read from PostgreSQL.
- Timestamps written to `timestamptz` columns are now timezone-aware (UTC).
- PostgreSQL connection strings assembled from discrete settings are URL-encoded.
- Pinned the ruff rule selection and version range (`ruff>=0.16,<0.17`) to keep `ruff check` stable across ruff releases.

### Fixed

- Fail fast with an actionable message when PostgreSQL is the only configured sink but the driver or connection details are missing (previously the run could appear to succeed while writing nothing).
- Accurate edge insert counts (`created`) via affected-row counts instead of attempted-row counts.

### Notes

- Non-breaking: the default sink and all existing CLI flags are unchanged. Dual-write is forward-only — existing SurrealDB data is not migrated to PostgreSQL automatically.

[Unreleased]: https://github.com/simonplmak-cloud/hkex-filing-scraper/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/simonplmak-cloud/hkex-filing-scraper/releases/tag/v1.1.0
