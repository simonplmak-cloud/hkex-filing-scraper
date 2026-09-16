# PostgreSQL Sink Guide

The scraper can mirror every record it persists into PostgreSQL, either as an alternative to SurrealDB or alongside it (dual-write).

## Enable it

1. Install the optional driver:

   ```bash
   pip install ".[postgres]"     # psycopg[binary,pool]>=3.1
   ```

2. Select the sink in `.env`:

   ```ini
   DATABASE_TARGET=postgres          # or "both"
   POSTGRES_DSN=postgresql://user:password@localhost:5432/hkex
   ```

   or the discrete form:

   ```ini
   DATABASE_TARGET=postgres
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5432
   POSTGRES_DATABASE=hkex
   POSTGRES_USER=hkex
   POSTGRES_PASSWORD=secret
   POSTGRES_SCHEMA=public        # optional, default "public"
   ```

3. Run as usual:

   ```bash
   hkex-scraper
   ```

The schema is created on startup with `CREATE TABLE IF NOT EXISTS` — no manual DDL, and safe to re-run.

## Schema

| Table | Key | Notes |
| ----- | --- | ----- |
| `exchange_filing` | `filing_id` (MD5-16, PK) | Metadata + document payload. `document_tables` → `jsonb`, `referenced_tickers` → `text[]`, `filing_date`/`updated_at` → `timestamptz`. |
| `scrape_coverage` | `(chunk_from, chunk_to, run_id)` unique | Chunk completeness rows. |
| `has_filing` | `(company_id, filing_id)` PK | Company → filing edges. |
| `references_filing` | `(filing_id, company_id)` PK | Filing → referenced-company edges. |

`filing_id` is the same identifier used by SurrealDB, so it is a reliable cross-sink join key.

## Idempotency

- Filings, coverage, and edges use `INSERT ... ON CONFLICT ... DO UPDATE` (or `DO NOTHING` for edges). Re-running the scraper updates existing rows and creates no duplicates.
- The metadata upsert never touches `document_*` columns, and document payloads use a scoped `UPDATE`, so a `--metadata-only` re-run will not overwrite extracted text or tables.

## Example queries

```sql
-- Filings per company
SELECT company_ticker, count(*) AS filings
FROM exchange_filing
GROUP BY company_ticker
ORDER BY filings DESC
LIMIT 20;

-- Filings that reference a ticker
SELECT filing_id, title, filing_date
FROM exchange_filing
WHERE referenced_tickers && ARRAY['0700.HK']
ORDER BY filing_date DESC;

-- Inspect extracted tables (JSONB)
SELECT filing_id, elem->>'pageNumber' AS page, left(elem->>'markdown', 200) AS preview
FROM exchange_filing,
     jsonb_array_elements(document_tables) AS elem
WHERE filing_type = 'Annual Report'
LIMIT 5;

-- Document processing status distribution
SELECT document_status, count(*) FROM exchange_filing GROUP BY 1;

-- Coverage report
SELECT chunk_from, chunk_to, api_count, ingested_count, unique_count, run_id
FROM scrape_coverage
ORDER BY chunk_from DESC;
```

## Operational notes

- **Failure isolation.** A PostgreSQL failure never aborts or rolls back the SurrealDB write (and vice versa). Per-sink success/failure counts are printed at the end of every run.
- **Graceful degradation.** If the driver is not installed or no connection details are configured, the scraper logs a warning and continues with the other sink. If PostgreSQL is the *only* configured sink and is unusable, the run fails fast with an actionable error.
- **Read routing.** With `DATABASE_TARGET=postgres`, Phase 2 pending-filing selection and graph ticker enumeration read from PostgreSQL rather than SurrealDB.
- **Credentials.** The DSN is never logged; `db_postgres` redacts `password=` and URL credentials from error text.
- **Edges.** Graph edges are only written when `COMPANY_TABLE` is configured. In dual mode an edge is created only for tickers matched in the SurrealDB company table; in PostgreSQL-only mode all tickers produce edges.
- **Pooling.** `POSTGRES_MIN_POOL` / `POSTGRES_MAX_POOL` size the connection pool (defaults 1 / 15).

## Parity

```bash
hkex-scraper --database-target both --parity-report
```

Prints SurrealDB and PostgreSQL filing counts and the difference (`Parity: OK` on zero). When only one sink is enabled, parity is reported as `N/A`.
