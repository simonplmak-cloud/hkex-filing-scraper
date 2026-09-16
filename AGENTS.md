# AGENTS.md

## Project overview

Python package (`hatchling` build, src-layout) that scrapes HKEx regulatory filings via an undocumented JSON API and ingests them into one or more database sinks (PostgreSQL, MySQL/MariaDB, SQLite, SurrealDB). Sink selection is config-driven via `DATABASE_TARGET` (an ordered, comma-separated list). CLI entrypoint: `hkex-scraper` → `src/hkex_scraper/main.py:main()`.

## Commands

```bash
pip install -e ".[dev,all]"      # dev install (editable + test/lint deps + doc extraction + postgres + mysql)
pip install .                     # minimal install (no doc extraction, no drivers; SQLite works)
pip install ".[postgres]"         # add the optional psycopg driver
pip install ".[mysql]"            # add the optional PyMySQL driver (MySQL/MariaDB)
pip install ".[duckdb]"           # or [mongodb], [clickhouse], [neo4j]
pytest                            # run all tests (no DB/network needed)
ruff check                        # lint (target: py310, line-length: 100)
```

`.env` is loaded from `Path.cwd()`, NOT the project root.

## Architecture

Two-phase pipeline orchestrated from `main.py`:

| Phase | What | Module |
|-------|------|--------|
| 1     | Scrape filing metadata from HKEx JSON API | `pipeline.py:run_phase1()` |
| 2     | Download documents + extract text/tables | `pipeline.py:run_phase2()` |
| —     | Graph edge creation (optional) | `graph.py` |
| —     | SurrealDB connection + schema DDL | `db.py` |
| —     | Uniform sink contract, registry, dialects, adapters | `sinks/` |

Phase 1 is always followed by Phase 2 unless `--metadata-only` is passed. `--backfill-docs` runs Phase 2 in isolation. The pipeline builds one canonical record per filing/document/coverage/edge and dispatches it to **every** sink in `config.sink_ids()`; reads are served by the first configured sink whose capabilities include `reads`. A failure on one sink never blocks another, and `pipeline.sink_exit_code()` is non-zero when any configured sink failed.

### Module map

| Module | Responsibility |
|--------|---------------|
| `main.py` | CLI arg parsing, orchestration, validation, per-sink schema init, N-way parity |
| `config.py` | Env vars + constants (loaded at import time from CWD `.env`); `sink_ids()` parses `DATABASE_TARGET` |
| `api.py` | HKEx JSON API session management, chunking, record parsing |
| `pipeline.py` | Phase 1 metadata save, Phase 2 download/extract/save loop; canonical records + sink dispatch |
| `extractor.py` | PDF/HTML/Excel text + table extraction → Markdown |
| `db.py` | SurrealDB `/sql` and `/rpc` query helpers, schema DDL, batch upsert |
| `db_postgres.py` | PostgreSQL implementation: `_AVAILABLE` guard, mirrored DDL, parameterised `ON CONFLICT` upserts, read helpers, credential redaction |
| `sinks/base.py` | `Sink` contract, `SinkCapabilities`, normalised error codes, `redact()` |
| `sinks/registry.py` | Lazy id→factory map with license/OSI/extra metadata |
| `sinks/dialects.py` | Per-dialect SQL: placeholders, quoting, types, upsert/DDL/select builders |
| `sinks/relational.py` | Shared relational engine (batching, redaction, degradation) |
| `sinks/postgres.py` / `mysql.py` / `sqlite.py` / `duckdb.py` | Relational adapters (PostgreSQL; MySQL/MariaDB; SQLite; DuckDB) |
| `sinks/mongodb.py` | Document sink (`$set` upserts, collections keyed on `_id`) |
| `sinks/clickhouse.py` | Columnar sink (`ReplacingMergeTree`, read-merge-reinsert, `FINAL` reads) |
| `sinks/neo4j.py` | Graph sink (`MERGE` nodes/relationships, Cypher reads) |
| `sinks/surrealdb.py` | SurrealDB adapter: SurrealQL, `RELATE`, RPC→`/sql` document fallback, company-id resolution |
| `graph.py` | `has_filing` and `references_filing` edge dispatch to every edge-capable sink |
| `utils.py` | Logging, string helpers, filing classification, ticker extraction, company-key helpers |

## SurrealDB quirks

- **Two endpoints**: `/sql` (1 MiB body limit, string-escaped SQL) and `/rpc` (4 MiB limit, parameterised JSON-RPC). Document saves go via `/rpc` first; fall back to `/sql` on failure.
- **Record-link parsing bug**: `/rpc` interprets JSON strings matching `word:word` as record links, corrupting `option<string>` fields. When this happens, the code falls back to `/sql` automatically (`sinks/surrealdb.py:_is_record_link_error()`).
- **Text truncation**: `documentText` gets pre-truncated if the RPC payload exceeds ~3.8 MB. Truncation lives in `sinks/surrealdb.py:SurrealDBSink.upsert_document()` and preserves table data when possible. Extreme cases fall through to `/sql` with further truncation and no tables.
- **SCHEMAFULL**: `exchange_filing` is SCHEMAFULL. New fields must be added to the schema DDL in `db.py:_build_schema_sql()`.
- **`scrape_coverage`** is a second SCHEMAFULL table (chunk-level `apiCount`/`ingestedCount`/`uniqueCount` + `runId`). Phase 1 writes one row per chunk; read back via `--coverage-report`.
- **Schema migration**: `documentContent` (obsolete blob field) is auto-removed on every startup via `initialize_schema()`.
- **`documentTables`** is `option<array<object>>` with individually typed sub-fields (`tableIndex`, `sheetName`, `pageNumber`, `headers`, `rowCount`, `markdown`).
- Run `initialize_schema()` safely at every startup — all DDL uses `IF NOT EXISTS`.

## Database sinks

`DATABASE_TARGET` is an ordered, comma-separated list of sink ids: `postgres`, `mysql`, `mariadb`, `sqlite`, `duckdb`, `mongodb`, `clickhouse`, `neo4j`, `surrealdb`. There is no default — an unset or unknown value fails fast.

- **Contract**: `sinks/base.py:Sink`. Every method returns `(value, error_code)` with `""` meaning success; no method raises for a driver/connection problem. Capability differences are declared in `SinkCapabilities`.
- **Registry**: `sinks/registry.py:SINKS` maps each id to `SinkSpec{label, license, source_available, extra, factory}`; factories import the sink module (and driver) lazily.
- **Relational sinks** (PostgreSQL, MySQL/MariaDB, SQLite, DuckDB) share `sinks/relational.py` + a `sinks/dialects.py:Dialect`. PostgreSQL keeps its dedicated `db_postgres.py` behind `PostgresSink` (pooling, `jsonb`, GIN). DuckDB has no `rowcount`, so its dialect appends `RETURNING 1`.
- **Idempotency**: PostgreSQL/SQLite/DuckDB use `ON CONFLICT DO UPDATE`, MySQL/MariaDB `ON DUPLICATE KEY UPDATE`, MongoDB `update_one(upsert=True)`, Neo4j `MERGE`, ClickHouse `ReplacingMergeTree` + read-merge-reinsert (`native_upsert=False`). A metadata upsert never touches `document_*` columns; a document write only updates an existing record.
- **MySQL/MariaDB**: MySQL driver is `PyMySQL` (extra `mysql`); the `mariadb` sink reads `MARIADB_*` and falls back to `MYSQL_*`. Indexes are declared inline because MySQL lacks `CREATE INDEX IF NOT EXISTS`.
- **SQLite / DuckDB**: SQLite uses stdlib `sqlite3`; DuckDB uses the `duckdb` extra. Both take a file path or `:memory:` (`SQLITE_PATH` / `DUCKDB_PATH`); datetimes/JSON are stored as ISO text / JSON.
- **MongoDB / Neo4j**: document and graph models. MongoDB stores `filing_id` as both `_id` and a field; Neo4j stores `documentTables` as a JSON string (no nested maps) and `referencedTickers` as a string array.
- **Schema mirror**: any new SurrealDB field must be added to `db.py:_build_schema_sql()`, `db_postgres.py:_build_postgres_schema_sql()`, and `sinks/dialects.py:Dialect._column_ddl()` (plus the MongoDB/Neo4j/ClickHouse field lists).
- **Read routing**: reads (pending filings, distinct tickers, titles, coverage) are served by the first configured sink whose capabilities include `reads`.
- **Failure isolation**: per-sink counters live in `pipeline.SINK_STATS`; every configured sink is required, so `pipeline.sink_exit_code()` is non-zero when any sink failed. The CLI prints a sink summary every run.
- **Credentials**: DSNs are never logged. `sinks/base.py:redact()` (and `db_postgres._redact()`) scrub `password=` and URL credentials from error text.

## HKEx API flow

The HKEx JSON API requires a JSF session established via:

1. `GET /search/titlesearch.xhtml` with query params
2. Extract `javax.faces.ViewState` from the HTML
3. `POST` the form with `from`/`to` dates and the ViewState
4. `GET /search/titleSearchServlet.do` (JSON endpoint) with `rowRange` pagination

See `api.py:fetch_chunk_via_api()`. The API limits searches to 1 month at a time when no stock code is specified — `generate_monthly_chunks()` splits date ranges accordingly.

## Development rules

- `requests` and `beautifulsoup4` are base deps (always available). PDF/Excel extraction libs and every database driver (`psycopg[binary,pool]`, `PyMySQL`, `duckdb`, `pymongo`, `clickhouse-connect`, `neo4j`) are optional — code uses graceful fallbacks with `_AVAILABLE` flags. SQLite uses the stdlib and needs no extra.
- `python-dotenv` is **not** in base deps — `config.py` gracefully falls back if it's missing. Install `[all]` extras to get it.
- Log files go to `logs/` in CWD. Failed SQL is appended to `logs/hkex_failed.sql`.
- Tests are in `tests/` and are pure unit tests — no DB or network. Run with plain `pytest`.
- `constitution.md` (repo root) is the authoritative VDD constitution: security constraints and banned patterns live there (e.g. `escape_sql()` mandatory on all `/sql` paths, no silent `except:`, `IF NOT EXISTS` on all DDL). Follow it over this file when they conflict.

## Documentation

`docs/` is the single source of truth. It is served as a site by `mkdocs.yml`
(`mkdocs build --strict` must pass) and mirrored to the GitHub wiki by
`scripts/mirror_wiki.py` + `.github/workflows/wiki.yml` on every push that touches `docs/`.

- **Style:** `docs/STYLE.md` — US English, sentence-case H2s, canonical term **sink** (a
  configured destination) vs **engine** (the database product). Markdown lint is enforced by
  `markdownlint-cli2` (`.markdownlint-cli2.jsonc`, CI job `markdown`).
- **Sink guides** live in `docs/sinks/` — one page per sink id (`postgres`, `mysql`,
  `sqlite`, `mongodb`, `mariadb` shares `mysql.md`, `neo4j`, `clickhouse`, `duckdb`,
  `surrealdb`). Order everywhere is the documented popularity order in
  `sinks/registry.py:POPULARITY_ORDER`; no sink is privileged.
- **Adding a doc:** add it to the `nav` in `mkdocs.yml` *and* to `SECTIONS` in
  `scripts/mirror_wiki.py`; `tests/test_docs_consistency.py` fails otherwise.
- **Link style:** relative links inside `docs/`; absolute URLs only for files outside
  `docs_dir` (README, CONTRIBUTING, LICENSE, `examples/`).
- **Community files:** `README.md`, `CONTRIBUTING.md`, `SECURITY.md`, `SUPPORT.md`,
  `GOVERNANCE.md`, `CODE_OF_CONDUCT.md`, `.github/PULL_REQUEST_TEMPLATE.md`,
  `.github/ISSUE_TEMPLATE/`, `.github/FUNDING.yml` — all follow `docs/STYLE.md`.
