# HKEx Filing Scraper — Docs

An open-source Python tool that scrapes 25+ years of Hong Kong Stock Exchange (HKEx)
regulatory filings and ingests them into PostgreSQL, MySQL/MariaDB, SQLite, DuckDB,
MongoDB, ClickHouse, Neo4j, or SurrealDB.

## Start here

- [Getting started](getting-started.md)
- [Database backends (support matrix)](backends/README.md)
- [Configuration reference](configuration.md)
- [CLI reference](cli.md)
- [Try it locally (`examples/`)](https://github.com/simonplmak-cloud/hkex-filing-scraper/tree/main/examples)

## Per-engine guides

- [PostgreSQL](postgresql.md)
- [MySQL / MariaDB](backends/mysql.md)
- [SQLite](backends/sqlite.md)
- [DuckDB](backends/duckdb.md)
- [MongoDB](backends/mongodb.md)
- [ClickHouse](backends/clickhouse.md)
- [Neo4j](backends/neo4j.md)
- SurrealDB — see [Architecture](architecture.md)

## Operations

- [Architecture](architecture.md)
- [Troubleshooting](troubleshooting.md)
- [Testing](testing.md)
- [De-risking register](de-risking.md)
- [Legal & Terms of Use](legal.md)

## Releasing

- [How to make a release](releasing.md)
- [Release automation reference](release-automation.md)
- [Upgrading](upgrading.md)

## Decisions

- [ADR 0001 — Versioning and release automation](adr/0001-versioning-and-release-automation.md)
- [ADR 0002 — Multi-backend sink architecture](adr/0002-multi-sink-architecture.md)
- [ADR 0003 — Sink support policy](adr/0003-sink-support-policy.md)

## Project

- [README](https://github.com/simonplmak-cloud/hkex-filing-scraper#readme)
- [Contributing](https://github.com/simonplmak-cloud/hkex-filing-scraper/blob/main/CONTRIBUTING.md)
- [Changelog](https://github.com/simonplmak-cloud/hkex-filing-scraper/blob/main/CHANGELOG.md)
