# Examples

Try the scraper against any supported database in a few minutes.

## 1. Start a local stack

```bash
cd examples
docker compose up -d          # postgres, mysql, mariadb, mongodb, clickhouse, neo4j, surrealdb
```

Wait for the health checks to settle (`docker compose ps`).

## 2. Point the scraper at one (or several)

```bash
cd ..                         # repo root
cp .env.example .env

# Pick any combination; order sets read precedence.
echo 'DATABASE_TARGET=postgres,sqlite' >> .env
echo 'POSTGRES_DSN=postgresql://hkex:hkex@localhost:5432/hkex' >> .env
echo 'SQLITE_PATH=hkex.db' >> .env

pip install ".[all]"
hkex-scraper --metadata-only --limit 50
```

Or use the helper, which writes an `.env` for the sink you name and runs a
metadata-only scrape:

```bash
./examples/quickstart.sh postgres
./examples/quickstart.sh sqlite
./examples/quickstart.sh mysql
```

## Connection strings for the local stack

| Sink | `.env` |
| ---- | ------ |
| `postgres` | `POSTGRES_DSN=postgresql://hkex:hkex@localhost:5432/hkex` |
| `mysql` | `MYSQL_HOST=127.0.0.1 MYSQL_PORT=3306 MYSQL_DATABASE=hkex MYSQL_USER=hkex MYSQL_PASSWORD=hkex` |
| `mariadb` | `MARIADB_HOST=127.0.0.1 MARIADB_PORT=3307 MARIADB_DATABASE=hkex MARIADB_USER=hkex MARIADB_PASSWORD=hkex` |
| `sqlite` | `SQLITE_PATH=hkex.db` |
| `duckdb` | `DUCKDB_PATH=hkex.duckdb` |
| `mongodb` | `MONGODB_URI=mongodb://localhost:27017 MONGODB_DATABASE=hkex` |
| `clickhouse` | `CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=8123 CLICKHOUSE_DATABASE=hkex CLICKHOUSE_USER=hkex CLICKHOUSE_PASSWORD=hkex` |
| `neo4j` | `NEO4J_URI=bolt://localhost:7687 NEO4J_USER=neo4j NEO4J_PASSWORD=hkexpassword` |
| `surrealdb` | `SURREAL_ENDPOINT=http://localhost:8000 SURREAL_PASSWORD=root` |

## 3. Tear down

```bash
cd examples
docker compose down -v
```

> These containers use throwaway development credentials and an in-memory
> SurrealDB. Never use them as-is in production.
