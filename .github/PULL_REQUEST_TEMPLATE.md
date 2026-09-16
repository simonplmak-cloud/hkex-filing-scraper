<!--
  Thanks for contributing. Keep the description focused on the why and the impact.
-->

## Summary

<!-- One or two sentences: what changes, and why. -->

## Type of change

- [ ] `feat` — new feature
- [ ] `fix` — bug fix
- [ ] `docs` — documentation only
- [ ] `refactor` — no behavior change
- [ ] `chore` — tooling, dependencies, maintenance
- [ ] `test` — tests only

## Related issues

<!-- e.g. Closes #123 -->

## How this was tested

- [ ] `ruff check && ruff format --check` passes
- [ ] `pytest -q` passes
- [ ] Added or updated tests for the change
- [ ] Verified against a live sink (which one? — PostgreSQL / MySQL / SQLite / DuckDB / MongoDB / ClickHouse / Neo4j / SurrealDB)

## Checklist

- [ ] New or changed sink fields are mirrored across every schema (SurrealDB, PostgreSQL, dialect DDL, MongoDB/Neo4j/ClickHouse)
- [ ] SQL is parameterised; no values interpolated; credentials are never logged
- [ ] Docs, templates, and `.env.example` updated (the docs consistency test covers this)
- [ ] `CHANGELOG.md` updated under `Unreleased`
- [ ] No secrets, tokens, or real credentials committed
