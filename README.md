# HKEx Filing Scraper

![HKEx Filing Scraper — SurrealDB or PostgreSQL](docs/social_preview.png)

[![CI](https://github.com/simonplmak-cloud/hkex-filing-scraper/actions/workflows/ci.yml/badge.svg)](https://github.com/simonplmak-cloud/hkex-filing-scraper/actions/workflows/ci.yml)
[![GitHub Release](https://img.shields.io/github/v/release/simonplmak-cloud/hkex-filing-scraper?color=green)](https://github.com/simonplmak-cloud/hkex-filing-scraper/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![SurrealDB](https://img.shields.io/badge/SurrealDB-FF00A0?logo=surrealdb&logoColor=white)](https://surrealdb.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org)

An open-source Python tool that scrapes 25+ years of Hong Kong Stock Exchange (HKEx) regulatory filings and ingests them into **SurrealDB, PostgreSQL, or both** — with full-text extraction from PDF/HTML/Excel documents, structured tables, coverage tracking, and optional graph linking.

It uses the undocumented HKEx JSON API directly, which is significantly faster and more reliable than browser-based scraping.

## Why this project

Regulatory filings are the raw substrate for research, compliance, and LLM/RAG systems, but getting a complete, faithful, provenance-preserving copy is tedious: you have to reverse-engineer the HKEx API, handle a JSF session and pagination, parse Chinese/English bilingual PDFs, extract tables, and survive payload limits and database quirks. This tool does all of that and hands you a clean corpus.

The **dual-store** design means you don't have to adopt a new database to use it: keep the graph-native SurrealDB model, or mirror everything into PostgreSQL so existing SQL/BI/dbt tooling can query it. Set one variable (`DATABASE_TARGET`) and the same run feeds either or both.

## Features

- **Fast API scraping** — direct HKEx JSON API, no browser/Selenium.
- **Full history** — every filing from April 1999 to today, with chunk-level coverage verification.
- **Document processing** — downloads PDF/HTML/Excel and extracts full text plus structured tables (Markdown).
- **Dual sink** — `DATABASE_TARGET=surrealdb` (default), `postgres`, or `both`. PostgreSQL mirrors filings, documents, coverage, and edges with idempotent `ON CONFLICT` upserts; `document_tables` is stored as `jsonb`.
- **Graph linking** (SurrealDB) — optional `(company)-[has_filing]->(filing)` and `(filing)-[references_filing]->(company)` edges.
- **Resilient & parallel** — batching, parallel downloads, recursive retries, stalled-job detection.
- **Failure isolation** — a failure on one sink never blocks or rolls back the other; per-sink counters are reported every run.
- **Optional dependencies** — core is `requests` + `beautifulsoup4`; PDF/Excel extraction and the PostgreSQL driver are extras with graceful fallback.

## Installation

The package is distributed via GitHub (not PyPI):

```bash
git clone https://github.com/simonplmak-cloud/hkex-filing-scraper.git
cd hkex-filing-scraper

# Recommended: PDF/Excel extraction + dotenv + PostgreSQL driver
pip install ".[all]"

# Minimal (metadata + HTML only, SurrealDB only)
pip install .

# Add only the PostgreSQL sink driver
pip install ".[postgres]"
```

Optional extras: `pdf`, `excel`, `postgres`, `all`, `dev`.

## Quick start

### SurrealDB (default)

```bash
cp .env.example .env
# edit .env with your SURREAL_* settings
hkex-scraper --metadata-only          # fast: metadata only
hkex-scraper                          # full: metadata + documents + graph
```

### PostgreSQL only

```bash
cp .env.example .env
# .env:
#   DATABASE_TARGET=postgres
#   POSTGRES_DSN=postgresql://user:password@localhost:5432/hkex
hkex-scraper
```

The schema is created automatically on startup — no manual DDL.

### Both (dual-write)

```bash
# .env:
#   DATABASE_TARGET=both
#   SURREAL_ENDPOINT=...  SURREAL_PASSWORD=...
#   POSTGRES_DSN=postgresql://user:password@localhost:5432/hkex
hkex-scraper --database-target both --parity-report
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

# Choose the sink for this run
hkex-scraper --database-target postgres
hkex-scraper --database-target both

# Reporting
hkex-scraper --coverage-report
hkex-scraper --database-target both --parity-report
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
| `--database-target TARGET` | Override `DATABASE_TARGET` (`surrealdb` \| `postgres` \| `both`). |
| `--coverage-report` | Print chunk coverage from the active sink, then exit. |
| `--parity-report` | Print filing counts per sink and the difference, then exit. |
| `--version` | Print the version and exit. |

Exit code is non-zero when a **required** sink recorded write failures (SurrealDB when enabled; PostgreSQL when it is the only sink).

## Configuration

Configuration is loaded from `.env` in the **current working directory** (not the project root). Full reference: [docs/configuration.md](docs/configuration.md).

| Variable | Default | Purpose |
| -------- | ------- | ------- |
| `DATABASE_TARGET` | `surrealdb` | `surrealdb`, `postgres`, or `both`. |
| `SURREAL_ENDPOINT` | — | SurrealDB HTTP endpoint (required when target includes surrealdb). |
| `SURREAL_NAMESPACE` / `SURREAL_DATABASE` | `default` | SurrealDB NS/DB. |
| `SURREAL_USERNAME` / `SURREAL_PASSWORD` | `root` / — | SurrealDB credentials. |
| `POSTGRES_DSN` | — | Full PostgreSQL DSN (preferred). |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `localhost` / `5432` | Discrete connection (used when `POSTGRES_DSN` is empty). |
| `POSTGRES_DATABASE` / `POSTGRES_USER` / `POSTGRES_PASSWORD` | — | Discrete connection. |
| `POSTGRES_SCHEMA` | `public` | Schema for the mirrored tables. |
| `POSTGRES_MIN_POOL` / `POSTGRES_MAX_POOL` | `1` / `15` | Connection pool sizing. |
| `COMPANY_TABLE` | — | SurrealDB company table; enables graph edges. |
| `COMPANY_ID_PATTERN` | `{code}_{exchange}` | Ticker → company record ID pattern. |
| `MAX_DOWNLOAD_WORKERS` | `15` | Parallel document downloads. |

## Database schema

### SurrealDB

`exchange_filing` is `SCHEMAFULL` and holds filing metadata plus the document payload (`documentText`, `documentTables`, `documentStatus`, …). Graph linking adds the `has_filing` and `references_filing` edge tables, and `scrape_coverage` records per-chunk completeness. See [docs/architecture.md](docs/architecture.md).

### PostgreSQL

When `DATABASE_TARGET` includes `postgres`, the scraper mirrors the same records into PostgreSQL. The schema is created automatically with idempotent DDL.

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
- [PostgreSQL sink guide](docs/postgresql.md)
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

Tests are pure unit tests. Integration tests that need PostgreSQL are skipped unless `POSTGRES_DSN` is set.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Please report security issues per [SECURITY.md](SECURITY.md).

## License

MIT — see [LICENSE](LICENSE). Commercial redistribution of HKEx data may require a licensed HKEx feed; see the project notes on Terms of Use.
