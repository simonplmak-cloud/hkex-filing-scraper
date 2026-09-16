# Testing

## Unit tests (no database)

```bash
pip install -e ".[dev]"
pytest -q                 # all tests; integration tests skip automatically
ruff check && ruff format --check
```

The default `pytest` run needs neither a database nor the network. SQLite needs no service,
so its contract tests (`tests/test_sqlite_integration.py`) run in-process on every
invocation. Server-backed integration tests (**PostgreSQL**, **SurrealDB**, **dual-write**)
are skipped unless the relevant connection variables are set.

## PostgreSQL integration

```bash
export DATABASE_TARGET=postgres
export POSTGRES_DSN=postgresql://hkex:hkex@localhost:5432/hkex
pip install -e ".[dev,postgres]"
pytest -q tests/test_postgres_integration.py
```

## SurrealDB integration

```bash
export DATABASE_TARGET=surrealdb
export SURREAL_ENDPOINT=http://localhost:8000
export SURREAL_NAMESPACE=test
export SURREAL_DATABASE=hkex
export SURREAL_USERNAME=root
export SURREAL_PASSWORD=root
export COMPANY_TABLE=company        # enables the graph-edge test
pytest -q tests/test_surrealdb_integration.py
```

A throwaway SurrealDB (in-memory):

```bash
docker run -d --name hkex-surreal -p 8000:8000 \
  surrealdb/surrealdb:v3.2.4 start --log warn --user root --pass root --bind 0.0.0.0:8000 memory
```

## SQLite (always on, no service)

```bash
pytest -q tests/test_sqlite_integration.py
```

To exercise a configured SQLite sink through the multi-sink dispatch path:

```bash
export DATABASE_TARGET=postgres,sqlite
export POSTGRES_DSN=postgresql://hkex:hkex@localhost:5432/hkex
export SQLITE_PATH=/tmp/hkex-it.db
pytest -q
```

## Multi-sink integration

```bash
export DATABASE_TARGET=postgres,surrealdb,sqlite
export SURREAL_ENDPOINT=http://localhost:8000
export SURREAL_NAMESPACE=test
export SURREAL_DATABASE=hkex
export SURREAL_USERNAME=root
export SURREAL_PASSWORD=root
export POSTGRES_DSN=postgresql://hkex:hkex@localhost:5432/hkex
export SQLITE_PATH=/tmp/hkex-it.db
pytest -q tests/test_postgres_integration.py tests/test_surrealdb_integration.py tests/test_dual_write_integration.py
```

Every configured sink must have its schema initialised; the integration fixtures do this
for the whole configured set (matching `main._init_schemas()`).

## What is covered

| Test file | Requires | Checks |
| --------- | -------- | ------ |
| `tests/test_utils.py` | nothing | helpers, classification, ticker extraction, company-id loading (incl. missing table) |
| `tests/test_coverage.py` | nothing | coverage statement/logging shape |
| `tests/test_config.py` | nothing | `DATABASE_TARGET` parsing, CLI override, MySQL/PostgreSQL connection helpers |
| `tests/test_registry.py` | nothing | lazy driver loading, valid ids, capability declarations, read routing |
| `tests/test_dialects.py` | nothing | per-dialect DDL, upsert/edge/coverage SQL, value encoding |
| `tests/test_relational_sink.py` | nothing | driver-missing/unconfigured degradation, credential redaction |
| `tests/test_postgres_sink.py` | nothing | PostgreSQL DDL, upserts, degradation, redaction, adapter |
| `tests/test_sink_dispatch.py` | nothing | multi-sink dispatch, failure isolation and counters, graph dispatch, parity report |
| `tests/test_sqlite_integration.py` | nothing | full SQLite contract: schema, idempotent upserts, no-clobber, edges, reads |
| `tests/test_postgres_integration.py` | `POSTGRES_DSN` | schema, idempotent upserts, JSONB/array round-trip, no-clobber, coverage, edge counts |
| `tests/test_surrealdb_integration.py` | `SURREAL_*` | schema, idempotent upserts, document payload, coverage, graph edges |
| `tests/test_dual_write_integration.py` | two or more sinks | the same filing lands in every configured sink; document status mirrors |

## CI

`.github/workflows/ci.yml` runs lint/format, unit tests on Python 3.10–3.13, and
integration jobs: PostgreSQL (service container), SurrealDB (`docker run`), and dual-write
(PostgreSQL + SurrealDB). SQLite and DuckDB-style in-process coverage runs in the plain test
job. The SurrealDB image is pinned to `v3.2.4`.
