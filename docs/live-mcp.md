# Live MCP gateway

The **live MCP gateway** exposes HKEx filings to MCP clients over HTTP, fetching fresh data
from the HKEx website on every call. It is the hosted companion to the [MCP server](mcp.md):
where that server runs over stdio and reads a *stored* corpus from a database sink, this
gateway is **stateless**, stores nothing, and needs no database.

Endpoint:

```text
https://hkex-listco-updates.ascent-partners.com/api/mcp
```

## Tools

| Tool | What it does |
| ---- | ------------ |
| `get_server_info` | Reports the gateway version, transport, and hard limits. Reads nothing. |
| `search_filings` | Searches live filings in a date window (at most 31 days), optionally filtered to one stock code. |
| `get_filing` | Downloads one HKEx document and extracts its text and tables. |

Every tool is **read-only**. The gateway never writes, never runs schema DDL, and never
fetches an arbitrary URL — `get_filing` accepts only HKEx document hosts.

## Limits

The model cannot raise these:

| Limit | Value |
| ----- | ----- |
| Search window | 31 days per call |
| `max_results` | 200 |
| Extracted text | 300,000 characters per document |
| Tables | 30 per document |

## Connecting a client

The gateway speaks **Streamable HTTP** (stateless, JSON responses) at `/mcp` and needs no
authentication.

**opencode** (`opencode.json`):

```json
{
  "mcp": {
    "servers": {
      "hkex-filings-live": {
        "type": "remote",
        "url": "https://hkex-listco-updates.ascent-partners.com/api/mcp"
      }
    }
  }
}
```

**Claude** — add it as a custom connector pointing at the endpoint URL, choosing
"no authentication".

**Cursor** (`~/.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "hkex-filings-live": {
      "url": "https://hkex-listco-updates.ascent-partners.com/api/mcp"
    }
  }
}
```

## Security

The gateway is a deliberately public, read-only API over public filings data. Its controls
are:

- **Read-only by construction** — three read tools; no write, SQL, or arbitrary-fetch surface.
- **SSRF allowlist** — documents are fetched only from HKEx hosts (`www1.hkexnews.hk`).
- **Origin validation** — requests carrying a disallowed `Origin` are rejected (MCP's
  DNS-rebinding mitigation).
- **No caching** — responses are sent with `Cache-Control: no-store`.
- **Edge rate limiting** — the deployment applies WAF rate limiting on `/api/mcp`.
- **Liveness** — `GET /api/healthz` returns `{"ok":true}` and reads nothing.
- **Bounded responses** — the limits above keep every response well within the platform body
  limit.

## Deployment

The gateway is a single Python function on Vercel, deployed from this repository alongside
the documentation site. The transport is implemented directly in
`src/hkex_scraper/live_mcp.py` (it needs no server SDK and no ASGI lifespan), and the Vercel
entry point is `api/mcp.py`.

The project is linked to GitHub, so deployment is automatic: pushes to `main` deploy to
production (`https://hkex-listco-updates.ascent-partners.com/`), and every other branch or
pull request deploys an isolated preview. The function runs in the Hong Kong region (`hkg1`)
with a Singapore failover (`sin1`), and it is configured with Fluid compute so document
extraction can run within the function's execution budget.

The document-extraction libraries (`PyMuPDF`, `pymupdf4llm`, `openpyxl`) are AGPL-3.0, so
they are not published dependencies of the distribution (see [Legal](legal.md)); the
deployment installs them through the `deploy` dependency group in `pyproject.toml`.
`pymupdf4llm` is pinned to a release that depends only on PyMuPDF — its 1.x line pulls an
additional layout model (onnxruntime and numpy) that exceeds the serverless bundle limit.

## See also

- [MCP server](mcp.md) — the stdio server over a stored corpus.
- [Configuration](configuration.md) — environment variables for the CLI and sinks.
