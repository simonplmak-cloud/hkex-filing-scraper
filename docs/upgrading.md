# Upgrading

## Unreleased → 1.1.0

Version 1.1.0 adds a PostgreSQL sink. **It is non-breaking:** if you do nothing, the scraper behaves exactly as before.

| Area | Change | Action required |
| ---- | ------ | --------------- |
| Default sink | Still SurrealDB | None |
| CLI flags | All existing flags unchanged; two added (`--database-target`, `--parity-report`) | None |
| Schema | PostgreSQL tables added; SurrealDB schema unchanged | None unless using PostgreSQL |
| Dependencies | `psycopg` added as an **optional** extra; base install unchanged | `pip install ".[postgres]"` only if you want PostgreSQL |

### Adopting PostgreSQL

1. Install the driver: `pip install ".[postgres]"`.
2. Add to `.env`:
   ```ini
   DATABASE_TARGET=postgres   # or "both"
   POSTGRES_DSN=postgresql://user:password@localhost:5432/hkex
   ```
3. Run `hkex-scraper` as usual. Tables are created automatically.

### Dual-write

Set `DATABASE_TARGET=both` and configure both sinks. Both are written on every run. Dual-write is **forward-only**: data already in SurrealDB is not migrated to PostgreSQL. To populate PostgreSQL for existing data, re-run the scraper for the desired range (and `--backfill-docs` for documents).

### Rollback

Set `DATABASE_TARGET=surrealdb` (or remove the variable) to return to the previous behaviour. PostgreSQL tables are untouched and can be dropped independently.
