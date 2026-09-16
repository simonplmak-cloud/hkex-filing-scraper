# Getting Started

## Requirements

- Python 3.10+
- One or both of:
  - a SurrealDB instance (default sink)
  - a PostgreSQL 13+ instance (optional sink)
- A `.env` file placed in the **current working directory** (the scraper loads `Path.cwd()/.env`, not the repo root).

## Install

```bash
git clone https://github.com/simonplmak-cloud/hkex-filing-scraper.git
cd hkex-filing-scraper
pip install ".[all]"      # PDF/Excel extraction + dotenv + PostgreSQL driver
```

Minimal installs:

```bash
pip install .              # metadata + HTML only, SurrealDB only
pip install ".[postgres]"  # add the PostgreSQL driver
```

## Configure

```bash
cp .env.example .env
```

### SurrealDB (default)

```ini
DATABASE_TARGET=surrealdb
SURREAL_ENDPOINT=http://localhost:8000
SURREAL_NAMESPACE=default
SURREAL_DATABASE=default
SURREAL_USERNAME=root
SURREAL_PASSWORD=your_password
```

### PostgreSQL

```ini
DATABASE_TARGET=postgres
POSTGRES_DSN=postgresql://user:password@localhost:5432/hkex
```

Or use the discrete `POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_DATABASE` / `POSTGRES_USER` / `POSTGRES_PASSWORD` variables instead of `POSTGRES_DSN`.

### Both

```ini
DATABASE_TARGET=both
SURREAL_ENDPOINT=http://localhost:8000
SURREAL_PASSWORD=your_password
POSTGRES_DSN=postgresql://user:password@localhost:5432/hkex
```

## First run

```bash
# Metadata only — fastest way to validate configuration
hkex-scraper --metadata-only --limit 100

# Full history (metadata + documents + graph)
hkex-scraper --full-history
```

The schema (SurrealDB and/or PostgreSQL) is created automatically on startup using idempotent DDL.

## Verify

```bash
hkex-scraper --coverage-report                       # chunk-level coverage
hkex-scraper --database-target both --parity-report  # per-sink filing counts
```

Exit code is non-zero if a **required** sink recorded write failures.

## Next steps

- [PostgreSQL sink guide](postgresql.md)
- [Configuration reference](configuration.md)
- [CLI reference](cli.md)
- [Troubleshooting](troubleshooting.md)
