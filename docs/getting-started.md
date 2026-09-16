# Getting Started

## Requirements

- Python 3.10+
- At least one supported database to write into:
  - a relational server — PostgreSQL 13+, MySQL 8, or MariaDB 10.5+
  - a filesystem database — SQLite or DuckDB (no server required)
  - a document/graph/columnar server — MongoDB 6+, ClickHouse 24+, Neo4j 5, or SurrealDB
- A `.env` file placed in the **current working directory** (the scraper loads `Path.cwd()/.env`, not the repo root).

See [Database backends](backends/README.md) for the support matrix, licences, and capability differences.

## Install

```bash
git clone https://github.com/simonplmak-cloud/hkex-filing-scraper.git
cd hkex-filing-scraper
pip install ".[all]"      # PDF/Excel extraction + dotenv + every database driver
```

Minimal installs:

```bash
pip install .              # metadata + HTML only; SQLite works out of the box
pip install ".[postgres]"  # add one driver at a time
pip install ".[duckdb]"    # or: mysql, mongodb, clickhouse, neo4j
```

## Configure

```bash
cp .env.example .env
```

### PostgreSQL (recommended)

```ini
DATABASE_TARGET=postgres
POSTGRES_DSN=postgresql://user:password@localhost:5432/hkex
```

Or use the discrete `POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_DATABASE` / `POSTGRES_USER` / `POSTGRES_PASSWORD` variables instead of `POSTGRES_DSN`.

### SQLite (no server required)

```ini
DATABASE_TARGET=sqlite
SQLITE_PATH=hkex.db
```

### SurrealDB

```ini
DATABASE_TARGET=surrealdb
SURREAL_ENDPOINT=http://localhost:8000
SURREAL_NAMESPACE=default
SURREAL_DATABASE=default
SURREAL_USERNAME=root
SURREAL_PASSWORD=your_password
```

### Multiple sinks

```ini
# Order matters: reads come from the first read-capable sink.
DATABASE_TARGET=postgres,sqlite
POSTGRES_DSN=postgresql://user:password@localhost:5432/hkex
SQLITE_PATH=hkex.db
```

### Other backends

```ini
# MySQL / MariaDB
DATABASE_TARGET=mysql
MYSQL_HOST=localhost  MYSQL_DATABASE=hkex  MYSQL_USER=hkex  MYSQL_PASSWORD=secret

# DuckDB (no server)
DATABASE_TARGET=duckdb
DUCKDB_PATH=hkex.duckdb

# MongoDB
DATABASE_TARGET=mongodb
MONGODB_URI=mongodb://localhost:27017  MONGODB_DATABASE=hkex

# ClickHouse
DATABASE_TARGET=clickhouse
CLICKHOUSE_HOST=localhost  CLICKHOUSE_DATABASE=hkex  CLICKHOUSE_USER=default

# Neo4j
DATABASE_TARGET=neo4j
NEO4J_URI=bolt://localhost:7687  NEO4J_USER=neo4j  NEO4J_PASSWORD=secret
```

`DATABASE_TARGET` is required — there is no implicit default. See
[Database backends](backends/README.md) for the full support matrix.

## First run

```bash
# Metadata only — fastest way to validate configuration
hkex-scraper --metadata-only --limit 100

# Full history (metadata + documents + graph)
hkex-scraper --full-history
```

The schema for every configured sink is created automatically on startup using idempotent DDL.

## Verify

```bash
hkex-scraper --coverage-report                          # chunk-level coverage
hkex-scraper --database-target postgres,sqlite --parity-report  # per-sink filing counts
```

Exit code is non-zero if **any** configured sink recorded write failures.

## Next steps

- [Database backends (support matrix)](backends/README.md)
- [PostgreSQL sink guide](postgresql.md)
- [Configuration reference](configuration.md)
- [CLI reference](cli.md)
- [Troubleshooting](troubleshooting.md)
