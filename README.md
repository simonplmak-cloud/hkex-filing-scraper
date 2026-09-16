# HKEx Filing Scraper

![HKEx Filing Scraper — SurrealDB or PostgreSQL](docs/social_preview.png)

[![CI](https://github.com/simonplmak-cloud/hkex-filing-scraper/actions/workflows/ci.yml/badge.svg)](https://github.com/simonplmak-cloud/hkex-filing-scraper/actions/workflows/ci.yml)
[![GitHub Release](https://img.shields.io/github/v/release/simonplmak-cloud/hkex-filing-scraper?color=green)](https://github.com/simonplmak-cloud/hkex-filing-scraper/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![SurrealDB](https://img.shields.io/badge/SurrealDB-FF00A0?logo=surrealdb&logoColor=white)](https://surrealdb.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org)
[![MySQL](https://img.shields.io/badge/MySQL-4479A1?logo=mysql&logoColor=white)](https://www.mysql.com)
[![SQLite](https://img.shields.io/badge/SQLite-003B57?logo=sqlite&logoColor=white)](https://sqlite.org)

An open-source Python tool that scrapes 25+ years of Hong Kong Stock Exchange (HKEx) regulatory filings and ingests them into **any combination of PostgreSQL, MySQL/MariaDB, SQLite, and SurrealDB** — with full-text extraction from PDF/HTML/Excel documents, structured tables, coverage tracking, and optional graph linking.

It uses the undocumented HKEx JSON API directly, which is significantly faster and more reliable than browser-based scraping.

## Why this project

Regulatory filings are the raw substrate for research, compliance, and LLM/RAG systems, but getting a complete, faithful, provenance-preserving copy is tedious: you have to reverse-engineer the HKEx API, handle a JSF session and pagination, parse Chinese/English bilingual PDFs, extract tables, and survive payload limits and database quirks. This tool does all of that and hands you a clean corpus.

The **multi-sink** design means you don't have to adopt a new database to use it: keep the graph-native SurrealDB model, or mirror everything into PostgreSQL, MySQL/MariaDB, or SQLite so existing SQL/BI/dbt tooling can query it. Set one variable (`DATABASE_TARGET`) and the same run feeds one or several sinks.

## Features

- **Fast API scraping** — direct HKEx JSON API, no browser/Selenium.
- **Full history** — every filing from April 1999 to today, with chunk-level coverage verification.
- **Document processing** — downloads PDF/HTML/Excel and extracts full text plus structured tables (Markdown).
- **Multi-sink** — write to any combination of PostgreSQL, MySQL/MariaDB, SQLite, and SurrealDB via `DATABASE_TARGET` (an ordered, comma-separated list). Relational sinks mirror filings, documents, coverage, and edges with idempotent upserts; `document_tables` is `jsonb` on PostgreSQL and JSON elsewhere.
- **Graph linking** (SurrealDB) — optional `(company)-[has_filing]->(filing)` and `(filing)-[references_filing]->(company)` edges.
- **Resilient & parallel** — batching, parallel downloads, recursive retries, stalled-job detection.
- **Failure isolation** — a failure on one sink never blocks or rolls back another; per-sink counters are reported every run, and the run exits non-zero if any configured sink failed.
- **Optional dependencies** — core is `requests` + `beautifulsoup4`; PDF/Excel extraction and the PostgreSQL/MySQL drivers are extras with graceful fallback. SQLite needs no extra.

## Installation

The package is distributed via GitHub (not PyPI):

```bash
git clone https://github.com/simonplmak-cloud/hkex-filing-scraper.git
cd hkex-filing-scraper

# Recommended: PDF/Excel extraction + dotenv + PostgreSQL + MySQL drivers
pip install ".[all]"

# Minimal (metadata + HTML only, SQLite or SurrealDB)
pip install .

# Add only the PostgreSQL sink driver
pip install ".[postgres]"

# Add only the MySQL/MariaDB sink driver
pip install ".[mysql]"
```

Optional extras: `pdf`, `excel`, `postgres`, `mysql`, `all`, `dev`. SQLite needs no extra.

## Quick start

### PostgreSQL (recommended default)

```bash
cp .env.example .env
# .env:
#   DATABASE_TARGET=postgres
#   POSTGRES_DSN=postgresql://user:password@localhost:5432/hkex
hkex-scraper --metadata-only          # fast: metadata only
hkex-scraper                          # full: metadata + documents + graph
```

### SQLite (no server required)

```bash
# .env:
#   DATABASE_TARGET=sqlite
#   SQLITE_PATH=hkex.db
hkex-scraper
```

### SurrealDB

```bash
# .env:
#   DATABASE_TARGET=surrealdb
#   SURREAL_ENDPOINT=http://localhost:8000
#   SURREAL_PASSWORD=root
hkex-scraper
```

The schema is created automatically on startup — no manual DDL.

### MySQL / MariaDB

```bash
# .env:
#   DATABASE_TARGET=mysql
#   MYSQL_HOST=localhost  MYSQL_DATABASE=hkex  MYSQL_USER=hkex  MYSQL_PASSWORD=secret
hkex-scraper
```

### Multiple sinks (order sets read precedence)

```bash
# .env:
#   DATABASE_TARGET=postgres,sqlite,surrealdb
#   ... connection settings for each ...
hkex-scraper --parity-report
```

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
| `--database-target SINKS` | Override `DATABASE_TARGET` (comma-separated; valid: `postgres`, `mysql`, `mariadb`, `sqlite`, `surrealdb`). |
| `--coverage-report` | Print chunk coverage from the read source, then exit. |
| `--parity-report` | Print filing counts per sink and the spread, then exit. |
| `--version` | Print the version and exit. |

Exit code is non-zero when **any** configured sink recorded write failures.

## Configuration

Configuration is loaded from `.env` in the **current working directory** (not the project root). Full reference: [docs/configuration.md](docs/configuration.md).

| Variable | Default | Purpose |
| -------- | ------- | ------- |
| `DATABASE_TARGET` | **required** | Comma-separated, ordered sink list: `postgres`, `mysql`, `mariadb`, `sqlite`, `surrealdb`. |
| `SURREAL_ENDPOINT` | — | SurrealDB HTTP endpoint (required when target includes surrealdb). |
| `SURREAL_NAMESPACE` / `SURREAL_DATABASE` | `default` | SurrealDB NS/DB. |
| `SURREAL_USERNAME` / `SURREAL_PASSWORD` | `root` / — | SurrealDB credentials. |
| `POSTGRES_DSN` | — | Full PostgreSQL DSN (preferred). |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `localhost` / `5432` | Discrete connection (used when `POSTGRES_DSN` is empty). |
| `POSTGRES_DATABASE` / `POSTGRES_USER` / `POSTGRES_PASSWORD` | — | Discrete connection. |
| `POSTGRES_SCHEMA` | `public` | Schema for the mirrored tables. |
| `POSTGRES_MIN_POOL` / `POSTGRES_MAX_POOL` | `1` / `15` | Connection pool sizing. |
| `MYSQL_DSN` | — | Full MySQL DSN (preferred); `MARIADB_DSN` follows the same shape. |
| `MYSQL_HOST` / `MYSQL_PORT` / `MYSQL_DATABASE` / `MYSQL_USER` / `MYSQL_PASSWORD` | — / `3306` / — | MySQL connection; `MARIADB_*` falls back to these. |
| `SQLITE_PATH` | — | SQLite file path, or `:memory:`. |
| `COMPANY_TABLE` | — | Company table; enables graph edges. |
| `COMPANY_ID_PATTERN` | `{code}_{exchange}` | Ticker → company key pattern. |
| `MAX_DOWNLOAD_WORKERS` | `15` | Parallel document downloads. |

## Database schema

### SurrealDB

`exchange_filing` is `SCHEMAFULL` and holds filing metadata plus the document payload (`documentText`, `documentTables`, `documentStatus`, …). Graph linking adds the `has_filing` and `references_filing` edge tables, and `scrape_coverage` records per-chunk completeness. See [docs/architecture.md](docs/architecture.md).

### Relational sinks (PostgreSQL, MySQL/MariaDB, SQLite)

When `DATABASE_TARGET` includes a relational sink, the scraper mirrors the same records into
it. The schema is created automatically with idempotent DDL. Per-engine guides:
[PostgreSQL](docs/postgresql.md), [MySQL/MariaDB](docs/backends/mysql.md),
[SQLite](docs/backends/sqlite.md).

| Table | Purpose |
| ----- | ------- |
| `exchange_filing` | Filing metadata + document payload. `filing_id` (MD5-16) is the primary key and the shared join key with SurrealDB. `document_tables` is `jsonb`; `referenced_tickers` is `text[]`. |
| `scrape_coverage` | One row per processed chunk, unique on `(chunk_from, chunk_to, run_id)`. |
| `has_filing` | Company → filing edges, primary key `(company_id, filing_id)`. |
| `references_filing` | Filing → referenced-company edges, primary key `(filing_id, company_id)`. |

Writes are parameterised `INSERT ... ON CONFLICT DO UPDATE` statements, so re-running is idempotent. Document payloads use a separate `UPDATE`, so a metadata-only re-run never overwrites extracted text or tables.

```sql
-- PostgreSQL examples
SELECT company_ticker, count(*) FROM exchange_filing GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

SELECT filing_id, title
FROM exchange_filing
WHERE referenced_tickers && ARRAY['0700.HK'];

SELECT filing_id, tbl->>'markdown'
FROM exchange_filing, jsonb_array_elements(document_tables) AS tbl
WHERE filing_type = 'Annual Report';
```

See [docs/postgresql.md](docs/postgresql.md) for setup, queries, and troubleshooting.

## Documentation

- [Getting started](docs/getting-started.md)
- [Database backends (support matrix)](docs/backends/README.md)
- [PostgreSQL sink guide](docs/postgresql.md)
- [MySQL/MariaDB sink guide](docs/backends/mysql.md)
- [SQLite sink guide](docs/backends/sqlite.md)
- [Configuration reference](docs/configuration.md)
- [CLI reference](docs/cli.md)
- [Architecture](docs/architecture.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Testing](docs/testing.md)
- [Upgrading](docs/upgrading.md)
- [Releasing](docs/releasing.md)
- [Release automation reference](docs/release-automation.md)
- [Changelog](CHANGELOG.md)

## Development

```bash
pip install -e ".[dev,all]"
ruff check          # lint (py310, line-length 100)
ruff format --check # formatting
pytest              # unit tests (no DB/network required)
```

Tests are pure unit tests. SQLite contract tests run in-process on every `pytest`; integration tests that need a server (PostgreSQL, SurrealDB, MySQL) are skipped unless that sink is configured.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Please report security issues per [SECURITY.md](SECURITY.md).

## License

MIT — see [LICENSE](LICENSE). Commercial redistribution of HKEx data may require a licensed HKEx feed; see the project notes on Terms of Use.
