# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/2.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Tier 2 sinks: MongoDB, DuckDB, ClickHouse, Neo4j.** Four more backends implement the same `Sink` contract, each an optional extra with a guarded import:
  - `duckdb` (MIT; `duckdb` extra) — in-process analytical SQL, `ON CONFLICT` upserts, JSON columns, PK-only schema.
  - `mongodb` (SSPL, source-available; `mongodb` extra) — document model, collections keyed on `_id`, `$set` upserts; metadata and document writes never touch each other's fields.
  - `clickhouse` (Apache-2.0; `clickhouse` extra) — columnar `ReplacingMergeTree`; declares `native_upsert=False` and uses read-merge-reinsert so metadata and document writes preserve each other; reads use `FINAL`.
  - `neo4j` (GPLv3 Community; `neo4j` extra) — graph model with `(:Company)-[:HAS_FILING]->(:Filing)` and `(:Filing)-[:REFERENCES_FILING]->(:Company)`, all writes via `MERGE`; `documentTables` stored as JSON.
- **Uniform sink architecture.** A new `sinks/` package defines one destination contract (`Sink`), declared capabilities (`SinkCapabilities`), and a lazy registry, so the pipeline, graph linker, and CLI no longer branch on a destination name. Backends are hand-written adapters plus a small SQL dialect descriptor; there is no ORM.
- **MySQL and MariaDB sinks** (`mysql`, `mariadb`) via the optional `PyMySQL` driver (`pip install ".[mysql]"`), with `ON DUPLICATE KEY UPDATE` upserts and `INSERT IGNORE` edge inserts.
- **SQLite sink** (`sqlite`) using the standard-library `sqlite3` driver — no extra dependency.
- Per-sink capability model (upsert, reads, edges, JSON, arrays, limits) and a support matrix at `docs/backends/README.md`.
- `docs/adr/0002-multi-sink-architecture.md`, `docs/adr/0003-sink-support-policy.md`, and per-backend guides under `docs/backends/` (mysql, sqlite, duckdb, mongodb, clickhouse, neo4j).
- CI job `integration-tier2` running MongoDB, ClickHouse, and Neo4j service containers.

### Changed

- **Breaking:** `DATABASE_TARGET` is now an explicit, ordered, comma-separated list of sink ids (`postgres`, `mysql`, `mariadb`, `sqlite`, `surrealdb`). The `both`/`dual` aliases and the implicit `surrealdb` default are removed; an unset or unknown value fails fast with an actionable message.
- **Breaking:** reads are served by the first configured sink that supports reads; there is no per-sink read branch and no `READ_SOURCE` variable.
- Every explicitly configured sink is now required: any sink's write failure marks the run non-zero (previously a non-sole PostgreSQL sink could fail without failing the run). Failure isolation is unchanged.
- `--parity-report` is now N-way: it prints a filing count per sink and the spread between the maximum and minimum.
- SurrealDB RPC body-size truncation moved from the pipeline into the SurrealDB sink; the pipeline now builds one canonical payload and each sink applies its own declared limit.

### Fixed

- Integration fixtures now initialise the schema of **every** configured sink (matching `main()`), fixing "no such table" failures when a second sink was configured during tests.

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
