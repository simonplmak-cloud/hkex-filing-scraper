<!-- markdownlint-disable MD034 -->

# Launch pack — v2.4.0 (HKEx filings, live for AI agents)

Internal marketing notes. Not part of the docs site (kept out of `docs/` on purpose).

## Canonical links

- Release: <https://github.com/simonplmak-cloud/hkex-filing-scraper/releases/tag/v2.4.0>
- What's new: <https://hkex-listco-updates.ascent-partners.com/news/>
- Agent setup: <https://hkex-listco-updates.ascent-partners.com/ai-agents/>
- Endpoint: `https://hkex-listco-updates.ascent-partners.com/api/mcp`
- Registry name: `io.github.simonplmak-cloud/hkex-filings`
- Attach asset: `docs/social_preview.png` (1280×640)

## Posting order

1. GitHub Release (done — v2.4.0) + Discussion announcement (canonical target). ✅
2. Registry publish + `awesome-mcp-servers` PR (directories mirror the registry). ✅
3. Hacker News → X → Reddit within ~24h, once the listing is live. HN skipped; X done; Reddit pending login.
4. LinkedIn later in the week (finance/compliance audience). ⏳

## Status (2026-09-20)

| Target | Status | Detail |
| ------ | ------ | ------ |
| Official MCP Registry | ✅ published | `io.github.simonplmak-cloud/hkex-filings` 2.4.0 — `streamable-http` remote + PyPI package; verified via the registry API |
| Smithery | ✅ live | `simon-pl-mak/hkex-filings` — deployed in 9s, 3 tools discovered (`get_server_info`, `search_filings`, `get_filing`), metadata filled, quality 73/100; <https://smithery.ai/servers/simon-pl-mak/hkex-filings> |
| GitHub Discussion | ✅ posted | Announcements #44 |
| `awesome-mcp-servers` | ✅ PR opened | #14715 (Finance & Fintech) |
| X / Twitter | ✅ posted | <https://x.com/SimonMak51642/status/2101483998161825960> — `social_preview.png` attached |
| Reddit r/mcp | ⏳ pending login | copy below approved; browser parked on `/r/mcp/submit` |
| Hacker News | ❌ skipped | no HN account; account creation declined |
| PulseMCP | ⏸ blocked upstream | submissions "temporarily paused" since 2026-09-03; they auto-ingest the official registry |
| Glama | ⏳ auto-index | not listed yet; adding requires an account, so relying on GitHub auto-index (`mcp-server` topic) |
| mcp.so | ❌ skipped | sole submission path is a $39 one-time fee |
| LinkedIn | ⏳ planned | later in the week |

Notes from the submissions:

- Smithery's public API lags the dashboard: `registry.smithery.ai/servers/simon-pl-mak/hkex-filings` still showed an empty description after the settings save persisted (score 49 → 73/100). Re-check in a few hours.
- Smithery's GitHub App requests write scopes (gists, starring, watching) — granted manually, not automated.
- Registry publish needed the GitHub device flow (code entered at <https://github.com/login/device>).

## Hacker News — Show HN

**Title**

> Show HN: HKEx filings MCP server – ask an AI agent about Hong Kong filings

**First comment**

> I've been scraping HKEx (Hong Kong Stock Exchange) regulatory filings for a while and
> kept hitting the same wall: the data is public but the API is undocumented (JSF session,
> one-month search windows, PDF-heavy documents).
>
> So I built two things on one Python package:
>
> 1. A pipeline that ingests 25+ years of filings into any of nine databases (Postgres,
>    MySQL/MariaDB, SQLite, Mongo, Neo4j, ClickHouse, DuckDB, SurrealDB).
> 2. An MCP server, so an agent can ask for filings directly. There's a hosted, stateless
>    endpoint with no API key (`POST https://hkex-listco-updates.ascent-partners.com/api/mcp`)
>    and a local stdio server for a corpus you keep yourself.
>
> Three read-only tools: `search_filings` (≤31-day window), `get_filing` (downloads and
> extracts text + tables), `get_server_info`. Nothing is stored; documents are fetched only
> from HKEx hosts. Live for HKEx data since April 1999.
>
> It's MIT, on PyPI, and in the MCP registry. Happy to answer anything about the API
> reverse-engineering or the transport (the gateway is a hand-rolled Streamable HTTP
> handler because the SDK's server needs a lifespan hook serverless doesn't give you).

## X / Twitter

**Single post**

> HKEx filings are now queryable from any AI agent.
>
> Hosted MCP endpoint, no API key, nothing stored:
> https://hkex-listco-updates.ascent-partners.com/api/mcp
>
> Ask: "list the filings published between 2026-09-01 and 2026-09-18 and summarise the
> interim report."
>
> MIT · also ingests 25+ years into nine databases. 🧵

**Thread (3–4 posts)**

> 1/ HKEx filings for AI agents. `search_filings`, `get_filing`, `get_server_info` — read-only,
> live from HKEx, no API key.
> 2/ Works with Claude, ChatGPT, Cursor, Copilot, Gemini CLI, opencode, Manus, Perplexity.
> One URL: https://hkex-listco-updates.ascent-partners.com/ai-agents/
> 3/ Prefer your own data? `pip install hkex-filing-scraper` writes 25+ years into any of nine
> databases, and `hkex-scraper-mcp` serves it locally over stdio.
> 4/ MIT, on PyPI, published to the official MCP registry:
> `io.github.simonplmak-cloud/hkex-filings`.

## Reddit — r/mcp

**Title**

> HKEx (Hong Kong Stock Exchange) filings as an MCP server — hosted, no API key, read-only

**Body**

> Sharing an MCP server for HKEx regulatory filings. Two ways to run it:
>
> - **Hosted:** `https://hkex-listco-updates.ascent-partners.com/api/mcp` (Streamable HTTP,
>   stateless, no auth). Three read-only tools — `search_filings` (≤31-day window),
>   `get_filing` (extracts text + tables from the PDF), `get_server_info`.
> - **Local:** `pip install "hkex-filing-scraper[mcp]"` → `hkex-scraper-mcp`, a 16-tool stdio
>   server over a corpus you scrape into any of nine databases.
>
> Nothing is written and no arbitrary URL is fetched (documents come only from HKEx hosts).
> Verified with the official Python and TypeScript SDK clients, `mcp-remote`, and opencode.
> MIT. Feedback welcome — especially on the tool surface.

Cross-post to **r/MCPservers** after a day if the first post lands; optional r/ClaudeAI.

## LinkedIn (finance/compliance audience)

> Regulatory filings are public, but making them *queryable* by an AI assistant usually means
> a data-engineering project. We packaged that away: an open-source tool that ingests 25+
> years of HKEx filings into the database you already run, plus a read-only MCP endpoint so an
> assistant can answer "what did company X file last quarter?" with a citation back to the
> original document. MIT-licensed, no hosted data retention.

## Registry publish (one-time, needs your GitHub login)

```bash
# install
curl -L "https://github.com/modelcontextprotocol/registry/releases/latest/download/mcp-publisher_$(uname -s | tr '[:upper:]' '[:lower:]')_$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/').tar.gz" | tar xz mcp-publisher && sudo mv mcp-publisher /usr/local/bin/
# publish (server.json is at the repo root and schema-validated)
mcp-publisher login github
mcp-publisher publish
# verify
curl -s "https://registry.modelcontextprotocol.io/v0/servers?search=hkex-filings" | head
```

The PyPI ownership check reads `mcp-name: io.github.simonplmak-cloud/hkex-filings` from the
2.4.0 README (confirmed present in the PyPI JSON).

## Metrics (baseline → launch)

| Metric | Before launch | After |
| ------ | ------------- | ----- |
| PyPI downloads / month | 110 | re-measure 7 days after launch |
| GitHub stars | 15 | re-measure 7 days after launch |
| Forks | 5 | re-measure 7 days after launch |
| Registry listing | pending | ✅ published 2026-09-20 (official registry + Smithery) |
