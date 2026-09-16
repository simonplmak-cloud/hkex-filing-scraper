# Security Policy

## Supported versions

| Version | Supported |
| ------- | --------- |
| 2.x (latest `main`) | Yes |
| 1.x | No |

Only the latest release on `main` receives security fixes. There are no backports.

## Reporting a vulnerability

**Do not open a public issue.** Email [simon.pl.mak@gmail.com](mailto:simon.pl.mak@gmail.com) with `SECURITY` in the
subject line, and include:

- the affected version or commit,
- a description of the issue and its impact,
- minimal reproduction steps or a proof of concept,
- any suggested remediation.

If you prefer, use GitHub's [private vulnerability reporting](https://github.com/simonplmak-cloud/hkex-filing-scraper/security/advisories/new).

## What to expect

| Stage | Target |
| ----- | ------ |
| Acknowledgement of your report | 3 business days |
| Initial assessment and severity | 7 business days |
| Fix or documented mitigation | 30 days for high/critical, next release otherwise |
| Public disclosure | Coordinated with you after a fix ships |

We credit reporters in the release notes unless you ask to stay anonymous.

## Scope

In scope:

- The published package and its CLI (`hkex-scraper`).
- Credential handling — DSNs and passwords must never appear in logs, errors, or the SQL
  failure log (`sinks/base.py:redact()`, `db_postgres._redact()`).
- SQL construction — all values must be parameterised; `escape_sql()` is mandatory on every
  SurrealDB `/sql` path.
- The GitHub Actions workflows in `.github/workflows/`.

Out of scope:

- The undocumented HKEx JSON API and any HKEx-hosted service.
- Vulnerabilities in third-party database engines or drivers themselves — report those
  upstream. We will still bump a pinned dependency when a fix is available.
- Issues that require an already-compromised host, database, or credentials.
- Rate limiting, scraping etiquette, or terms-of-use questions — see [docs/legal.md](docs/legal.md).

## Safe harbor

We will not pursue legal action against researchers who make a good-faith effort to follow
this policy, avoid privacy violations and service disruption, and give us reasonable time to
respond before public disclosure.

## Hardening already in place

- Parameterised SQL everywhere; per-sink redaction of credentials in error paths.
- Secret scanning, CodeQL, and Dependabot enabled on the repository.
- Least-privilege `GITHUB_TOKEN` permissions per workflow.
