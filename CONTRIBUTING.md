# Contributing

Thanks for considering a contribution. This project scrapes a public regulatory data source and aims to be a trustworthy, complete, and maintainable ETL tool.

## Development setup

```bash
git clone https://github.com/simonplmak-cloud/hkex-filing-scraper.git
cd hkex-filing-scraper
pip install -e ".[dev,all]"
```

## Checks

```bash
ruff check           # lint (py310, line-length 100)
ruff format --check  # formatting
pytest               # unit tests — no database or network required
```

All three must pass. Tests are pure unit tests; integration tests that need PostgreSQL are skipped unless `POSTGRES_DSN` is set.

## Guidelines

- **Python 3.10+**, type hints, `from __future__ import annotations`.
- Keep core dependencies to `requests` + `beautifulsoup4`. New dependencies belong in an optional extra behind a graceful `_AVAILABLE` guard.
- Never hardcode or log credentials. Database connection strings must never appear in logs or error messages.
- Never silently swallow errors — every failure path logs via `utils.log()`.
- All SQL must be parameterised. SurrealDB `/sql` string values must go through `utils.escape_sql()`.
- DDL must be idempotent (`IF NOT EXISTS`).

## Adding a persisted field

The two schemas are mirrored and must stay in sync:

1. Add a `DEFINE FIELD IF NOT EXISTS` to `db.py:_build_schema_sql()`.
2. Add the matching column to `db_postgres.py:_build_postgres_schema_sql()` (and the relevant upsert column list).
3. Update the field tables in `README.md` and `docs/`.
4. Add a test.

## Commits

- Concise, imperative mood, describe intent rather than mechanics.
- Keep commits single-purpose; avoid mixing formatting sweeps with behaviour changes.

## Pull requests

- Fork, branch from `main`, and open a PR.
- Fill in the PR template and link any related issue.
- Ensure CI (lint, format, tests) is green.

## Reporting bugs

Use the issue templates. For security issues, follow [SECURITY.md](SECURITY.md) instead of opening a public issue.
