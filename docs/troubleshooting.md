# Troubleshooting

## Configuration

**`ERROR: Missing required env vars: SURREAL_ENDPOINT, SURREAL_PASSWORD`**
`DATABASE_TARGET` includes `surrealdb` but SurrealDB settings are unset. Set them, or use `DATABASE_TARGET=postgres`.

**`PostgreSQL is the only configured sink but the psycopg driver is not installed`**
```bash
pip install ".[postgres]"
```

**`.env` is not being read**
`.env` is loaded from the **current working directory**, not the repository root. Run the CLI from the directory that contains `.env`, or export the variables.

## PostgreSQL

**`PostgreSQL sink enabled but no connection details are configured`**
Set `POSTGRES_DSN` (preferred) or `POSTGRES_DATABASE` + `POSTGRES_USER` (+ `POSTGRES_PASSWORD`).

**`POSTGRES_WRITE_ERROR` on document save**
The `UPDATE` matched no row — the filing metadata was not written first. Run a metadata pass (`--metadata-only`) or a full run so the row exists.

**Tables are not created**
Schema creation runs on startup only when the sink is enabled and available. Check the log line `PostgreSQL schema initialized successfully`. If it reports a warning and PostgreSQL is the only sink, the run exits `1` with the reason.

**`POSTGRES_PARITY_MISMATCH`**
The two sinks differ in filing count. Common causes: enabling PostgreSQL after a SurrealDB-only history (no backfill — dual-write is forward-only), or a failed write on one sink (see the per-sink failure counts). Run `--parity-report` for the totals.

**Graph edges missing in PostgreSQL**
Edge creation requires `COMPANY_TABLE`. In dual mode, edges are created only for tickers matched in the SurrealDB company table; in PostgreSQL-only mode all tickers produce edges.

## SurrealDB

**`/rpc` record-link error / `expected a option<...>`**
SurrealDB interprets strings shaped like `word:word` as record links. The scraper detects this and retries via `/sql` automatically.

**Document saved with fewer tables than expected**
The `/rpc` (≈3.8 MB) or `/sql` (≈1 MB) body limits were hit and the payload was truncated. This is logged and recorded in `documentStatusReason` (e.g. `truncated_from_...`).

## Scraping

**Coverage gap (ingested < API total)**
Re-run the same range; deduplication means only missing filings are added. `--coverage-report` shows per-chunk counts.

**Downloads skipped**
Documents over 25 MB, unsupported types, or HTTP errors are recorded as `skipped`/`failed` with a reason such as `too_large`, `unsupported_type`, or `http_404`.

**No documents processed**
Phase 2 only processes filings with a `documentUrl` and no `documentStatus`. Use `--metadata-only` first, then `--backfill-docs`.

## Tests

**Integration tests are skipped**
They require `POSTGRES_DSN` to be set; without it they skip by design so the default `pytest` run needs no database.

**`ruff check` fails after upgrading ruff**
Ruff ≥ 0.16 enables many more rules by default. This project pins an explicit `select` and a ruff version range in `pyproject.toml`; install the pinned version (`pip install -e ".[dev]"`).
