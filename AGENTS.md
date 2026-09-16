# AGENTS.md

## Project overview

Python package (`hatchling` build, src-layout) that scrapes HKEx regulatory filings via an undocumented JSON API and ingests them into SurrealDB and/or PostgreSQL. Sink selection is config-driven via `DATABASE_TARGET`. CLI entrypoint: `hkex-scraper` → `src/hkex_scraper/main.py:main()`.

## Commands

```bash
pip install -e ".[dev,all]"      # dev install (editable + test/lint deps + PDF/Excel/doc extraction + postgres)
pip install .                     # minimal install (no doc extraction, no postgres)
pip install ".[postgres]"         # add the optional psycopg driver
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
| —     | Schema init + query helpers | `db.py` |
| —     | Optional PostgreSQL sink (DDL + upserts + reads) | `db_postgres.py` |

Phase 1 is always followed by Phase 2 unless `--metadata-only` is passed. `--backfill-docs` runs Phase 2 in isolation. Every persistence write is dispatched to each sink in `config.DATABASE_TARGET`; a failure on one sink never blocks the other (`pipeline.sink_exit_code()` is non-zero only for required sinks).

### Module map

| Module | Responsibility |
|--------|---------------|
| `main.py` | CLI arg parsing, orchestration, validation |
| `config.py` | Env vars + constants (loaded at import time from CWD `.env`) |
| `api.py` | HKEx JSON API session management, chunking, record parsing |
| `pipeline.py` | Phase 1 metadata save, Phase 2 download/extract/save loop |
| `extractor.py` | PDF/HTML/Excel text + table extraction → Markdown |
| `db.py` | SurrealDB `/sql` and `/rpc` query helpers, schema DDL, batch upsert |
| `db_postgres.py` | Optional PostgreSQL sink: `_AVAILABLE` guard, mirrored DDL, parameterised `ON CONFLICT` upserts, read helpers, credential redaction |
| `graph.py` | `has_filing` and `references_filing` edge creation |
| `utils.py` | Logging, string helpers, filing classification, ticker extraction |

## SurrealDB quirks

- **Two endpoints**: `/sql` (1 MiB body limit, string-escaped SQL) and `/rpc` (4 MiB limit, parameterised JSON-RPC). Document saves go via `/rpc` first; fall back to `/sql` on failure.
- **Record-link parsing bug**: `/rpc` interprets JSON strings matching `word:word` as record links, corrupting `option<string>` fields. When this happens, the code falls back to `/sql` automatically (`pipeline.py:_is_record_link_error()`).
- **Text truncation**: `documentText` gets pre-truncated if the RPC payload exceeds ~3.8 MB. Truncation preserves table data when possible. Extreme cases fall through to `/sql` with further truncation and no tables.
- **SCHEMAFULL**: `exchange_filing` is SCHEMAFULL. New fields must be added to the schema DDL in `db.py:_build_schema_sql()`.
- **`scrape_coverage`** is a second SCHEMAFULL table (chunk-level `apiCount`/`ingestedCount`/`uniqueCount` + `runId`). Phase 1 writes one row per chunk; read back via `--coverage-report`.
- **Schema migration**: `documentContent` (obsolete blob field) is auto-removed on every startup via `initialize_schema()`.
- **`documentTables`** is `option<array<object>>` with individually typed sub-fields (`tableIndex`, `sheetName`, `pageNumber`, `headers`, `rowCount`, `markdown`).
- Run `initialize_schema()` safely at every startup — all DDL uses `IF NOT EXISTS`.

## PostgreSQL sink (optional)

- **Guarded import**: `db_postgres.py` imports `psycopg` behind a try/except; when absent, `postgres_available()` is `False` and every entry point returns `POSTGRES_DRIVER_MISSING` instead of raising.
- **Idempotent upserts**: filings/coverage/edges use `INSERT ... ON CONFLICT <key> DO UPDATE ...` (metadata upsert never touches `document_*` columns); documents use a scoped `UPDATE ... WHERE filing_id = %s`.
- **Schema mirror**: `db_postgres.py:_build_postgres_schema_sql()` mirrors `exchange_filing`, `scrape_coverage`, `has_filing`, and `references_filing`. Any new SurrealDB field must be added to both DDL builders (`db.py:_build_schema_sql()` and `db_postgres.py:_build_postgres_schema_sql()`).
- **JSONB**: SurrealDB `option<array<object>>` (`documentTables`) is stored as `jsonb`; `option<array<string>>` as `text[]`; `option<datetime>` as `timestamptz`.
- **Read routing**: when `DATABASE_TARGET=postgres`, Phase 2 pending-filing selection and graph ticker enumeration read from PostgreSQL (`fetch_pending_filings`, `distinct_company_tickers`, `fetch_filing_ids_by_ticker`, `fetch_titles`) instead of SurrealDB.
- **Failure isolation**: per-sink counters live in `pipeline.SINK_STATS`; `pipeline.sink_exit_code()` returns non-zero only for required sinks; the CLI prints a sink summary every run.
- **Credentials**: the DSN is never logged. `db_postgres._redact()` scrubs `password=` and URL credentials from error text.

## HKEx API flow

The HKEx JSON API requires a JSF session established via:

1. `GET /search/titlesearch.xhtml` with query params
2. Extract `javax.faces.ViewState` from the HTML
3. `POST` the form with `from`/`to` dates and the ViewState
4. `GET /search/titleSearchServlet.do` (JSON endpoint) with `rowRange` pagination

See `api.py:fetch_chunk_via_api()`. The API limits searches to 1 month at a time when no stock code is specified — `generate_monthly_chunks()` splits date ranges accordingly.

## Development rules

- `requests` and `beautifulsoup4` are base deps (always available). PDF/Excel extraction libs and the PostgreSQL driver (`psycopg[binary,pool]`) are optional — code uses graceful fallbacks with `_AVAILABLE` flags.
- `python-dotenv` is **not** in base deps — `config.py` gracefully falls back if it's missing. Install `[all]` extras to get it.
- Log files go to `logs/` in CWD. Failed SQL is appended to `logs/hkex_failed.sql`.
- Tests are in `tests/` and are pure unit tests — no DB or network. Run with plain `pytest`.
- `constitution.md` (repo root) is the authoritative VDD constitution: security constraints and banned patterns live there (e.g. `escape_sql()` mandatory on all `/sql` paths, no silent `except:`, `IF NOT EXISTS` on all DDL). Follow it over this file when they conflict.
