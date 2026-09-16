# HKEx Filing Scraper

![HKEx Filing Scraper — one scraper, many databases](docs/social_preview.png)

[![CI](https://github.com/simonplmak-cloud/hkex-filing-scraper/actions/workflows/ci.yml/badge.svg)](https://github.com/simonplmak-cloud/hkex-filing-scraper/actions/workflows/ci.yml)
[![GitHub Release](https://img.shields.io/github/v/release/simonplmak-cloud/hkex-filing-scraper?color=green)](https://github.com/simonplmak-cloud/hkex-filing-scraper/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Ruff](https://img.shields.io/badge/lint-ruff-261230.svg)](https://github.com/astral-sh/ruff)
[![Docs](https://img.shields.io/badge/docs-simonplmak--cloud.github.io-blue)](https://simonplmak-cloud.github.io/hkex-filing-scraper/)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org)
[![MySQL](https://img.shields.io/badge/MySQL-4479A1?logo=mysql&logoColor=white)](https://www.mysql.com)
[![SQLite](https://img.shields.io/badge/SQLite-003B57?logo=sqlite&logoColor=white)](https://sqlite.org)
[![MongoDB](https://img.shields.io/badge/MongoDB-47A248?logo=mongodb&logoColor=white)](https://www.mongodb.com)
[![Neo4j](https://img.shields.io/badge/Neo4j-4581C3?logo=neo4j&logoColor=white)](https://neo4j.com)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-FFCC01?logo=clickhouse&logoColor=black)](https://clickhouse.com)
[![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?logo=duckdb&logoColor=black)](https://duckdb.org)
[![SurrealDB](https://img.shields.io/badge/SurrealDB-FF00A0?logo=surrealdb&logoColor=white)](https://surrealdb.com)

An open-source Python tool that scrapes 25+ years of Hong Kong Stock Exchange (HKEx)
regulatory filings and ingests them into **any combination of PostgreSQL, MySQL/MariaDB,
SQLite, MongoDB, Neo4j, ClickHouse, DuckDB, and SurrealDB** — with full-text extraction from
PDF/HTML/Excel documents, structured tables, coverage tracking, and optional graph linking.

It uses the undocumented HKEx JSON API directly, which is significantly faster and more
reliable than browser-based scraping.

## Contents

- [Database support](#database-support)
- [Why this project](#why-this-project)
- [How it works](#how-it-works)
- [Features](#features)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Usage](#usage)
- [Configuration](#configuration)
- [Database schema](#database-schema)
- [Documentation](#documentation)
- [Development](#development)
- [Contributing](#contributing)
- [Roadmap](#roadmap)
- [License](#license)

## Database support

Set `DATABASE_TARGET` to any ordered, comma-separated combination of these. **Every sink is a
first-class destination** — the order only decides which one serves reads. The full matrix
(licenses, capability differences, per-engine notes) is in
[docs/sinks](docs/sinks/README.md); rows are in the documented popularity order.

| Sink | Model | License | Extra | Idempotent upsert |
| ---- | ----- | ------- | ----- | ----------------- |
| `postgres` | relational | PostgreSQL License | `postgres` | `ON CONFLICT DO UPDATE` |
| `mysql` / `mariadb` | relational | GPLv2 | `mysql` | `ON DUPLICATE KEY UPDATE` |
| `sqlite` | relational | Public domain | — | `ON CONFLICT DO UPDATE` |
| `mongodb` | document | SSPL¹ | `mongodb` | `update_one(upsert=True)` |
| `neo4j` | graph | GPLv3 (Community) | `neo4j` | `MERGE` |
| `clickhouse` | columnar | Apache-2.0 | `clickhouse` | `ReplacingMergeTree` + read-merge |
| `duckdb` | relational | MIT | `duckdb` | `ON CONFLICT DO UPDATE` |
| `surrealdb` | graph + document | BSL 1.1¹ | — | `UPSERT` / `RELATE` |

¹ Source-available, not OSI-approved — labelled exceptions per [ADR 0003](docs/adr/0003-sink-support-policy.md).

## Why this project

Regulatory filings are the raw substrate for research, compliance, and LLM/RAG systems, but getting a complete, faithful, provenance-preserving copy is tedious: you have to reverse-engineer the HKEx API, handle a JSF session and pagination, parse Chinese/English bilingual PDFs, extract tables, and survive payload limits and database quirks. This tool does all of that and hands you a clean corpus.

The **multi-sink** design means you don't have to adopt a new database to use it: keep whichever store your team already runs, mirror everything into a second one for SQL/BI/dbt tooling, stream documents into a document store, or load a columnar engine for analytics. Set one variable (`DATABASE_TARGET`) and the same run feeds one or several sinks.

## How it works

The scraper runs in two phases, builds one canonical record per filing, and hands each record
to **every sink** listed in `DATABASE_TARGET`. Reads are served by the first configured sink
that supports them.

```mermaid
flowchart LR
    A[HKEx JSON API] --> B[Phase 1: metadata]
    B --> C[Canonical record]
    C --> D{DATABASE_TARGET}
    D --> E[(PostgreSQL)]
    D --> F[(MySQL / MariaDB)]
    D --> G[(SQLite)]
    D --> H[(MongoDB)]
    D --> I[(Neo4j)]
    D --> J[(ClickHouse)]
    D --> K[(DuckDB)]
    D --> L[(SurrealDB)]
    B --> M[Graph linking]
    M --> D
    B --> N[Phase 2: download and extract]
    N --> C
```

- **Phase 1** scrapes filing metadata from the undocumented HKEx JSON API through a JSF
  session, splitting the range into monthly chunks and deduplicating on a 16-character MD5
  `filingId`.
- **Graph linking** writes `has_filing` and `references_filing` edges to every edge-capable
  sink when `COMPANY_TABLE` is set.
- **Phase 2** downloads each filing's PDF/HTML/Excel document, extracts text and tables to
  Markdown, and writes the payload to every sink.
- **Failure isolation** means one sink's failure is logged and counted but never blocks
  another; the run exits non-zero if any configured sink failed.

More detail: [Architecture](docs/architecture.md) and [ADR 0002](docs/adr/0002-multi-sink-architecture.md).

## Features

- **Fast API scraping** — direct HKEx JSON API, no browser/Selenium.
- **Full history** — every filing from April 1999 to today, with chunk-level coverage verification.
- **Document processing** — downloads PDF/HTML/Excel and extracts full text plus structured tables (Markdown).
- **Multi-sink** — write to any combination of the nine sinks via `DATABASE_TARGET` (an ordered, comma-separated list). Each sink mirrors filings, documents, coverage, and edges with idempotent upserts, in its own native model.
- **Graph linking** — optional `(company)-[has_filing]->(filing)` and `(filing)-[references_filing]->(company)` edges on every edge-capable sink.
- **Parallel and resumable** — batching, parallel downloads, stalled-job detection, and per-chunk coverage tracking.
- **Failure isolation** — a failure on one sink never blocks or rolls back another; per-sink counters are reported every run, and the run exits non-zero if any configured sink failed.
- **Optional dependencies** — core is `requests` + `beautifulsoup4`; document extraction and every database driver (`psycopg`, `PyMySQL`, `duckdb`, `pymongo`, `clickhouse-connect`, `neo4j`) are extras with graceful fallback. SQLite needs no extra.

## Installation

The package is distributed via GitHub (not PyPI):

```bash
git clone https://github.com/simonplmak-cloud/hkex-filing-scraper.git
cd hkex-filing-scraper

# Excel extraction + dotenv + every database driver (permissive licenses only)
pip install ".[all]"

# Minimal (metadata + HTML only; SQLite works out of the box)
pip install .

# Add one sink driver at a time
pip install ".[postgres]"
pip install ".[mysql]"     # MySQL and MariaDB
pip install ".[duckdb]"    # or: mongodb, clickhouse, neo4j
```

Optional extras: `excel`, `postgres`, `mysql`, `duckdb`, `mongodb`, `clickhouse`, `neo4j`,
`all`, `dev`. SQLite and SurrealDB need no extra.

For a fully locked development environment, `uv.lock` pins every dependency including extras:

```bash
uv sync --frozen --all-extras
```

Releases carry signed build provenance and a CycloneDX SBOM — see
[Verifying a release](docs/releasing.md#verifying-a-release).

```bash
# PDF text + table extraction. AGPL-3.0 — see the license note below.
pip install ".[pdf]"
```

> **License note.** The `pdf` extra installs **PyMuPDF** and **pymupdf4llm**, which are
> **AGPL-3.0** (or a commercial license from Artifex). They are deliberately **not** part of
> `all`, so the default install stays permissive. If you distribute or host a service that
> includes them, the AGPL's network clause applies to you. See
> [docs/legal.md](docs/legal.md#third-party-licenses).

## Quick start

```bash
cp .env.example .env
```

Set `DATABASE_TARGET` to any ordered, comma-separated list of sink ids, then add that sink's
connection settings. Every sink is written to; reads come from the first read-capable sink.

```ini
# PostgreSQL
DATABASE_TARGET=postgres
POSTGRES_DSN=postgresql://user:password@localhost:5432/hkex

# MySQL / MariaDB
DATABASE_TARGET=mysql
MYSQL_HOST=localhost  MYSQL_DATABASE=hkex  MYSQL_USER=hkex  MYSQL_PASSWORD=secret

# SQLite (no server)
DATABASE_TARGET=sqlite
SQLITE_PATH=hkex.db

# MongoDB
DATABASE_TARGET=mongodb
MONGODB_URI=mongodb://localhost:27017  MONGODB_DATABASE=hkex

# Neo4j
DATABASE_TARGET=neo4j
NEO4J_URI=bolt://localhost:7687  NEO4J_USER=neo4j  NEO4J_PASSWORD=secret

# ClickHouse
DATABASE_TARGET=clickhouse
CLICKHOUSE_HOST=localhost  CLICKHOUSE_DATABASE=hkex  CLICKHOUSE_USER=default

# DuckDB (no server)
DATABASE_TARGET=duckdb
DUCKDB_PATH=hkex.duckdb

# SurrealDB
DATABASE_TARGET=surrealdb
SURREAL_ENDPOINT=http://localhost:8000
SURREAL_PASSWORD=root
```

Then run:

```bash
hkex-scraper --metadata-only          # fast: metadata only
hkex-scraper                          # full: metadata + documents + graph
```

### Several sinks at once

```ini
# Order sets read precedence.
DATABASE_TARGET=postgres,sqlite
POSTGRES_DSN=postgresql://user:password@localhost:5432/hkex
SQLITE_PATH=hkex.db
```

The schema is created automatically on startup — no manual DDL. See
[docs/sinks](docs/sinks/README.md) for the capability matrix. MongoDB (SSPL) and SurrealDB
(BSL) are source-available, labelled exceptions.

## Usage

```bash
# Last ~2 months (default)
hkex-scraper

# Full history from 1999
hkex-scraper --full-history

# Specific range
hkex-scraper --from-date 01/01/2024 --to-date 31/01/2024

# Metadata only / documents only / graphs only
hkex-scraper --metadata-only
hkex-scraper --backfill-docs
hkex-scraper --link-only

# Limit and dry-run
hkex-scraper --limit 500
hkex-scraper --dry-run

# Choose the sink(s) for this run (comma-separated, ordered)
hkex-scraper --database-target postgres
hkex-scraper --database-target sqlite
hkex-scraper --database-target postgres,sqlite

# Reporting
hkex-scraper --coverage-report
hkex-scraper --database-target postgres,sqlite --parity-report
```

### Command-line options

| Flag | Description |
| ---- | ----------- |
| `--full-history` | Scrape all filings from April 1999 to today. |
| `--from-date DD/MM/YYYY` | Start date for scraping. |
| `--to-date DD/MM/YYYY` | End date for scraping. |
| `--limit N` | Limit processing to `N` filings (0 = unlimited). |
| `--metadata-only` | Phase 1 only: metadata, no downloads. |
| `--backfill-docs` | Phase 2 only: download documents for existing filings. |
| `--link-only` | Only create/refresh graph edges. |
| `--dry-run` | Fetch but do not write to any database. |
| `--database-target SINKS` | Override `DATABASE_TARGET` (comma-separated; valid: `postgres`, `mysql`, `sqlite`, `mongodb`, `mariadb`, `neo4j`, `clickhouse`, `duckdb`, `surrealdb`). |
| `--coverage-report` | Print chunk coverage from the read source, then exit. |
| `--parity-report` | Print filing counts per sink and the spread, then exit. |
| `--version` | Print the version and exit. |

Exit code is non-zero when **any** configured sink recorded write failures.

## Configuration

Configuration is loaded from `.env` in the **current working directory** (not the project root). Full reference: [docs/configuration.md](docs/configuration.md).

| Variable | Default | Purpose |
| -------- | ------- | ------- |
| `DATABASE_TARGET` | **required** | Comma-separated, ordered sink list: `postgres`, `mysql`, `sqlite`, `mongodb`, `mariadb`, `neo4j`, `clickhouse`, `duckdb`, `surrealdb`. |
| `POSTGRES_DSN` | — | Full PostgreSQL DSN (preferred). |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `localhost` / `5432` | Discrete connection (used when `POSTGRES_DSN` is empty). |
| `POSTGRES_DATABASE` / `POSTGRES_USER` / `POSTGRES_PASSWORD` | — | Discrete connection. |
| `POSTGRES_SCHEMA` | `public` | Schema for the mirrored tables. |
| `POSTGRES_MIN_POOL` / `POSTGRES_MAX_POOL` | `1` / `15` | Connection pool sizing. |
| `MYSQL_DSN` | — | Full MySQL DSN (preferred); `MARIADB_DSN` follows the same shape. |
| `MYSQL_HOST` / `MYSQL_PORT` / `MYSQL_DATABASE` / `MYSQL_USER` / `MYSQL_PASSWORD` | — / `3306` / — | MySQL connection; `MARIADB_*` falls back to these. |
| `SQLITE_PATH` | — | SQLite file path, or `:memory:`. |
| `MONGODB_URI` / `MONGODB_DATABASE` | — | MongoDB connection URI and database. |
| `NEO4J_URI` / `NEO4J_USER` / `NEO4J_PASSWORD` / `NEO4J_DATABASE` | — | Neo4j Bolt connection. |
| `CLICKHOUSE_HOST` / `CLICKHOUSE_PORT` / `CLICKHOUSE_DATABASE` / `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | — / `8123` / — | ClickHouse connection. |
| `DUCKDB_PATH` | — | DuckDB file path, or `:memory:`. |
| `SURREAL_ENDPOINT` | — | SurrealDB HTTP endpoint (required when target includes surrealdb). |
| `SURREAL_NAMESPACE` / `SURREAL_DATABASE` | `default` | SurrealDB NS/DB. |
| `SURREAL_USERNAME` / `SURREAL_PASSWORD` | `root` / — | SurrealDB credentials. |
| `COMPANY_TABLE` | — | Company table; enables graph edges. |
| `COMPANY_ID_PATTERN` | `{code}_{exchange}` | Ticker → company key pattern. |
| `MAX_DOWNLOAD_WORKERS` | `15` | Parallel document downloads. |

## Database schema

Each sink stores the same four record families in its own native model. The schema is created
automatically with idempotent DDL, and `filing_id` (16-char MD5) is the shared join key across
all sinks.

| Record | Relational (`postgres`, `mysql`, `mariadb`, `sqlite`, `duckdb`) | Key |
| ------ | --------------------------------------------------------------- | --- |
| Filing + document payload | `exchange_filing` | `filing_id` |
| Chunk coverage | `scrape_coverage` | `(chunk_from, chunk_to, run_id)` |
| Company → filing edges | `has_filing` | `(company_id, filing_id)` |
| Filing → referenced-company edges | `references_filing` | `(filing_id, company_id)` |

Column shapes differ by engine: `document_tables` is `jsonb` (PostgreSQL), `json` (MySQL/MariaDB),
JSON text (SQLite/DuckDB/ClickHouse), an embedded array (MongoDB), or a JSON string (Neo4j);
`referenced_tickers` is `text[]` (PostgreSQL), `json` (MySQL), JSON text (SQLite/DuckDB),
`Array(String)` (ClickHouse), an array (MongoDB), or a string array (Neo4j). SurrealDB uses
`SCHEMAFULL` tables of the same names.

Writes are parameterised upserts, so re-running is idempotent. Document payloads use a separate
statement, so a metadata-only re-run never overwrites extracted text or tables.

```sql
-- PostgreSQL examples; each sink guide has engine-specific queries.
SELECT company_ticker, count(*) FROM exchange_filing GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

SELECT filing_id, title
FROM exchange_filing
WHERE referenced_tickers && ARRAY['0700.HK'];

SELECT filing_id, tbl->>'markdown'
FROM exchange_filing, jsonb_array_elements(document_tables) AS tbl
WHERE filing_type = 'Annual Report';
```

## Documentation

- [Getting started](docs/getting-started.md)
- **Docs site:** <https://simonplmak-cloud.github.io/hkex-filing-scraper/>
- [Database sinks (support matrix)](docs/sinks/README.md)
- [PostgreSQL sink guide](docs/sinks/postgresql.md)
- [MySQL/MariaDB sink guide](docs/sinks/mysql.md)
- [SQLite sink guide](docs/sinks/sqlite.md)
- [MongoDB sink guide](docs/sinks/mongodb.md)
- [Neo4j sink guide](docs/sinks/neo4j.md)
- [ClickHouse sink guide](docs/sinks/clickhouse.md)
- [DuckDB sink guide](docs/sinks/duckdb.md)
- [SurrealDB sink guide](docs/sinks/surrealdb.md)
- [Try it locally (`examples/`)](examples/README.md)
- [Configuration reference](docs/configuration.md)
- [CLI reference](docs/cli.md)
- [Architecture](docs/architecture.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Testing](docs/testing.md)
- [De-risking register](docs/de-risking.md)
- [Legal & Terms of Use](docs/legal.md)
- [Upgrading](docs/upgrading.md)
- [Releasing](docs/releasing.md)
- [Release automation reference](docs/release-automation.md)
- [Documentation style guide](docs/STYLE.md)
- [Changelog](CHANGELOG.md)

## Development

```bash
pip install -e ".[dev,all]"
ruff check          # lint (py310, line-length 100)
ruff format --check # formatting
pytest              # unit tests (no DB/network required)
```

Tests are pure unit tests. SQLite and DuckDB contract tests run in-process on every `pytest`;
integration tests that need a server are skipped unless that sink is configured.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Please report security issues per [SECURITY.md](SECURITY.md).
Ideas and questions are welcome in [Discussions](https://github.com/simonplmak-cloud/hkex-filing-scraper/discussions).

If this saves you time, a star helps others find it.

## Roadmap

- **Shipped** — nine sinks on one contract: PostgreSQL, MySQL/MariaDB, SQLite, MongoDB, Neo4j, ClickHouse, DuckDB, SurrealDB.
- **In progress** — hardening: recorded HKEx API fixtures + canary, HTTP retry/backoff, `--verify` cross-sink reconciliation, fault-injection tests, SBOM/provenance (see [docs/de-risking.md](docs/de-risking.md)).
- **Considered** — OpenSearch, Cassandra, Valkey, TiDB (see [ADR 0003](docs/adr/0003-sink-support-policy.md)).

Contributions that fit the roadmap, and `good first issue` items, are especially welcome.

## License

MIT — see [LICENSE](LICENSE). That covers **this project's code only**. Optional dependencies
carry their own licenses — notably the `pdf` extra, which is AGPL-3.0; see
[docs/legal.md](docs/legal.md#third-party-licenses).

**Data & Terms of Use:** this is a research tool for the undocumented HKEx JSON API. Commercial
redistribution of HKEx data may require a licensed HKEx feed; see [docs/legal.md](docs/legal.md).
