# CLI Reference

```
hkex-scraper [options]
```

## Options

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--full-history` | flag | off | Scrape all filings from April 1999 to today. |
| `--from-date` | `DD/MM/YYYY` | ~2 months ago | Start of the scrape range. |
| `--to-date` | `DD/MM/YYYY` | today | End of the scrape range. |
| `--limit N` | int | `0` (unlimited) | Process at most `N` filings. |
| `--metadata-only` | flag | off | Phase 1 only — no document downloads. |
| `--backfill-docs` | flag | off | Phase 2 only — process documents for filings already in the database. |
| `--link-only` | flag | off | Only create/refresh graph edges. |
| `--dry-run` | flag | off | Fetch data but write nothing. |
| `--database-target TARGET` | `surrealdb`\|`postgres`\|`both` | from `DATABASE_TARGET` | Override the sink for this run. |
| `--coverage-report` | flag | off | Print chunk coverage from the active sink, then exit. |
| `--parity-report` | flag | off | Print per-sink filing counts and difference, then exit. Requires `postgres`. |

## Behaviour

- With no mode flag, a full run is performed: Phase 1 (metadata), then graph linking, then Phase 2 (documents) — unless `--metadata-only`.
- `--backfill-docs` and `--link-only` run phases in isolation for incremental maintenance.
- A `--limit` also caps the number of documents Phase 2 processes.

## Exit codes

| Code | Meaning |
| ---- | ------- |
| `0` | Success (no required-sink failures). Failures on an *optional* sink are logged and counted but do not change the exit code. |
| `1` | A required sink failed (SurrealDB when enabled; PostgreSQL when it is the only sink), or configuration is invalid/unusable. |

## Examples

```bash
# Backfill documents only, 50 at a time
hkex-scraper --backfill-docs --limit 50

# Rebuild graph edges from existing filings
hkex-scraper --link-only

# Trial PostgreSQL without touching SurrealDB
hkex-scraper --database-target postgres --limit 100

# Validate config without writing
hkex-scraper --dry-run --limit 10

# Coverage and parity
hkex-scraper --coverage-report
hkex-scraper --database-target both --parity-report
```

## Logs

Each run writes a timestamped log to `logs/hkex_filings_<YYYY-MM-DD_HH-MM>.log` in the working directory, and failed SurrealDB statements are appended to `logs/hkex_failed.sql`. Credentials are never written to either file.
