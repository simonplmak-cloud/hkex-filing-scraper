# Contributing

## Setup

```bash
git clone https://github.com/simonplmak-cloud/hkex-filing-scraper.git
cd hkex-filing-scraper
pip install -e ".[dev]"
```

## Development

1. Create a feature branch from `main`
2. Make changes following project conventions
3. Verify: `pytest`
4. Open a PR against `main`

Integration tests for PostgreSQL, SurrealDB, and dual-write are skipped unless the
related connection variables are set — see [docs/testing.md](docs/testing.md).

## Commit Conventions

- `feat:` new features
- `fix:` bug fixes
- `chore:` maintenance
- `docs:` documentation

## Releasing

A release is created by pushing a tag. The **Release** workflow builds the package,
checks it, and attaches the files to the GitHub Release.

```bash
git tag -a v1.2.0 -m "v1.2.0"
git push origin v1.2.0
```

- Simple, step-by-step instructions: [docs/releasing.md](docs/releasing.md)
- Full reference (how it works and how to fix it): [docs/release-automation.md](docs/release-automation.md)

## Questions?

Open an issue. For security issues, see [SECURITY.md](./SECURITY.md).
