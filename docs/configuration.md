# Configuration Reference

Configuration is read from environment variables. A `.env` file in the **current working directory** is loaded automatically (requires `python-dotenv`; if it is missing, environment variables still work). `.env` is never committed.

## Database sink selection

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `DATABASE_TARGET` | `surrealdb` | Which sink(s) to write: `surrealdb`, `postgres`, or `both`. Aliases: `postgresql`/`pg`, `dual`. Unrecognised values fall back to `surrealdb`. |

## SurrealDB

Required when `DATABASE_TARGET` includes `surrealdb`.

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `SURREAL_ENDPOINT` | — | HTTP endpoint, e.g. `http://localhost:8000`. |
| `SURREAL_NAMESPACE` | `default` | Namespace. |
| `SURREAL_DATABASE` | `default` | Database. |
| `SURREAL_USERNAME` | `root` | Username. |
| `SURREAL_PASSWORD` | — | Password. |

## PostgreSQL

Required when `DATABASE_TARGET` includes `postgres`.

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `POSTGRES_DSN` | — | Full DSN, e.g. `postgresql://user:pass@host:5432/db`. Takes precedence over discrete settings. |
| `POSTGRES_HOST` | `localhost` | Host. |
| `POSTGRES_PORT` | `5432` | Port. |
| `POSTGRES_DATABASE` | — | Database name. |
| `POSTGRES_USER` | — | User. |
| `POSTGRES_PASSWORD` | — | Password (URL-encoded automatically when assembled from discrete settings). |
| `POSTGRES_SCHEMA` | `public` | Schema for the mirrored tables. |
| `POSTGRES_MIN_POOL` | `1` | Minimum pool connections. |
| `POSTGRES_MAX_POOL` | `15` | Maximum pool connections. |

## Graph linking

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `COMPANY_TABLE` | — | SurrealDB company table name. When empty, graph linking is disabled. |
| `COMPANY_ID_PATTERN` | `{code}_{exchange}` | Pattern that converts a ticker (`0451.HK`) into a company record ID (`451_HK`). |

## Performance

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `MAX_DOWNLOAD_WORKERS` | `15` | Parallel document downloads. |

## Precedence

1. `--database-target` command-line override (highest).
2. Environment variables / `.env`.
3. Built-in defaults.

## Constants (not configurable)

| Constant | Value | Meaning |
| -------- | ----- | ------- |
| `MAX_DOWNLOAD_SIZE` | 25 MB | Documents larger than this are skipped (`too_large`). |
| `MAX_SQL_BODY_SIZE` | ~900 KB | SurrealDB `/sql` body limit. |
| `MAX_RPC_BODY_SIZE` | ~3.8 MB | SurrealDB `/rpc` body limit. |
| `BATCH_SIZE` | 100 | Metadata batch size. |
