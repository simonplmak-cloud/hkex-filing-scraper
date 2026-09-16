# Contributing

Thanks for taking the time to contribute. This project follows a [Code of Conduct](CODE_OF_CONDUCT.md) — by participating you agree to uphold it.

## Ways to contribute

- **Report a bug** — open an issue with the [bug report template](.github/ISSUE_TEMPLATE/bug_report.yml).
- **Request a feature** — open an issue with the [feature request template](.github/ISSUE_TEMPLATE/feature_request.yml).
- **Add a sink** — adapters are small; start with [Adding a sink](docs/sinks/README.md#adding-a-sink) and [ADR 0003](docs/adr/0003-sink-support-policy.md).
- **Improve the docs** — see [docs/STYLE.md](docs/STYLE.md) for the house style.
- **Ask a question** — use [Discussions](https://github.com/simonplmak-cloud/hkex-filing-scraper/discussions), or see [SUPPORT.md](SUPPORT.md).

## Development setup

```bash
git clone https://github.com/simonplmak-cloud/hkex-filing-scraper.git
cd hkex-filing-scraper
pip install -e ".[dev,all]"
```

`.[dev]` is enough for the unit suite. `.[all]` adds the document-extraction libraries and
every database driver, which the optional integration tests need.

## Development workflow

1. Create a feature branch from `main` (`feat/…`, `fix/…`, `docs/…`, `chore/…`).
2. Make the change, following the conventions below.
3. Run the checks locally:

   ```bash
   ruff check && ruff format --check
   pytest -q
   ```

4. Open a pull request against `main` and fill in the template.

Tests are pure unit tests and need no database or network. SQLite and DuckDB integration
tests run in-process on every `pytest`; server-backed sinks (PostgreSQL, MySQL/MariaDB,
MongoDB, ClickHouse, Neo4j, SurrealDB) are skipped unless the matching connection variables
are set — see [docs/testing.md](docs/testing.md).

## Conventions

- **Python 3.10+**, `ruff` linted (`py310`, line length 100) and `ruff format`ed.
- **Parameterised SQL only.** Never interpolate values into a statement; `escape_sql()` is
  mandatory on every SurrealDB `/sql` path. See [constitution.md](constitution.md).
- **Never log credentials.** Route error text through `sinks/base.py:redact()`.
- **Optional dependencies stay optional** — guard each driver import with an `_AVAILABLE`
  flag and degrade gracefully.
- **A new sink field must be mirrored** across the SurrealDB schema, the PostgreSQL schema,
  the dialect column DDL, and the MongoDB/Neo4j/ClickHouse field lists; the
  [docs consistency test](tests/test_docs_consistency.py) enforces that the docs, templates,
  and `.env.example` stay in step.
- **Docs** follow [docs/STYLE.md](docs/STYLE.md) (US English, sentence-case headings, the term
  "sink" for a configured destination).

## Commit message conventions

Use [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` a new feature
- `fix:` a bug fix
- `docs:` documentation only
- `chore:` maintenance, tooling, dependencies
- `refactor:` a change that neither fixes a bug nor adds a feature
- `test:` tests only

Write the subject in the imperative mood (`add ClickHouse sink`, not `added`). Keep it under
72 characters; put the why in the body.

## Releasing

A release is created by pushing an annotated tag. The **Release** workflow builds the
distribution, runs the smoke test, and attaches the wheel, sdist, and `SHA256SUMS` to the
GitHub Release.

```bash
git tag -a v2.0.1 -m "v2.0.1"
git push origin v2.0.1
```

- Step-by-step guide: [docs/releasing.md](docs/releasing.md)
- Full reference: [docs/release-automation.md](docs/release-automation.md)

## Questions

Open an issue, or start a [Discussion](https://github.com/simonplmak-cloud/hkex-filing-scraper/discussions). For security issues, follow [SECURITY.md](SECURITY.md) instead of opening a public issue.
