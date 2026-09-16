<!--
  Title: short summary of the change.
  Keep the checklist accurate; CI must be green before merge.
-->

## Summary

<!-- What does this PR do, and why? -->

## Related issue

<!-- e.g. Closes #123 -->

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Documentation
- [ ] Refactor / internal
- [ ] CI / tooling

## Checklist

- [ ] `ruff check` passes
- [ ] `ruff format --check` passes
- [ ] `pytest` passes
- [ ] Docs updated (`README.md` / `docs/`) if behaviour or configuration changed
- [ ] `CHANGELOG.md` updated under `[Unreleased]`
- [ ] For a new persisted field: both `db.py:_build_schema_sql()` and `db_postgres.py:_build_postgres_schema_sql()` updated
- [ ] No credentials, tokens, or `.env` contents included

## Testing notes

<!-- How did you verify? Include commands and relevant output. -->

## Screenshots (if UI/visual change)

<!-- Optional. -->
