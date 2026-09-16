# Testing

## Unit tests (no database)

```bash
pip install -e ".[dev]"
pytest -q                 # all tests; integration tests skip automatically
ruff check && ruff format --check
```

The default `pytest` run needs neither a database nor the network. Integration tests
(**PostgreSQL**, **SurrealDB**, **dual-write**) are skipped unless the relevant
connection variables are set.

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

## Dual-write integration

```bash
export DATABASE_TARGET=both
export SURREAL_ENDPOINT=http://localhost:8000
export SURREAL_NAMESPACE=test
export SURREAL_DATABASE=hkex
export SURREAL_USERNAME=root
export SURREAL_PASSWORD=root
export POSTGRES_DSN=postgresql://hkex:hkex@localhost:5432/hkex
pytest -q tests/test_postgres_integration.py tests/test_surrealdb_integration.py tests/test_dual_write_integration.py
```

## What is covered

| Test file | Requires | Checks |
| --------- | -------- | ------ |
| `tests/test_utils.py` | nothing | helpers, classification, ticker extraction, company-id loading (incl. missing table) |
| `tests/test_coverage.py` | nothing | coverage statement/logging shape |
| `tests/test_postgres_sink.py` | nothing | DDL, upserts, degradation, redaction, fail-fast, edge counts |
| `tests/test_postgres_integration.py` | `POSTGRES_DSN` | schema, idempotent upserts, JSONB/array round-trip, no-clobber, coverage, edge counts |
| `tests/test_surrealdb_integration.py` | `SURREAL_*` | schema, idempotent upserts, document payload, coverage, graph edges |
| `tests/test_dual_write_integration.py` | both sinks | the same filing lands in both; document status mirrors |

## CI

`.github/workflows/ci.yml` runs lint/format, unit tests on Python 3.10–3.13, and three
integration jobs: PostgreSQL (service container), SurrealDB (`docker run`), and
dual-write (both). The SurrealDB image is pinned to `v3.2.4`.
