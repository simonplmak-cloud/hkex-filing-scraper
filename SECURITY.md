# Security Policy

## Supported versions

| Version | Supported |
| ------- | --------- |
| 1.1.x   | ✅ |
| < 1.1   | ❌ |

## Reporting a vulnerability

Please **do not** open a public issue for security problems. Instead, use GitHub's private reporting:

1. Go to the repository's **Security** tab.
2. Click **Report a vulnerability** (GitHub Security Advisories).
3. Describe the issue, the impact, and steps to reproduce.

You can expect an acknowledgement within a few days. Please give us a reasonable window to release a fix before public disclosure.

## Scope and handling of secrets

- All credentials are read from environment variables or `.env` and are never committed.
- The PostgreSQL DSN and SurrealDB credentials are never logged; `db_postgres` redacts credentials from error text.
- Extracted document text is treated as untrusted data and is never executed.
- Scraping is subject to the HKEx Terms of Use; this tool is a research artifact. Do not use it for commercial redistribution of HKEx data without a licensed feed.

## Out of scope

- Issues caused by running with credentials that grant broader access than intended.
- Vulnerabilities in third-party dependencies (report those upstream, though we appreciate a heads-up).
