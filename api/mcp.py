"""Vercel entry point for the live HKEx MCP gateway (legacy Streamable HTTP, stateless).

Deployed by the Vercel Python runtime. A catch-all POST route makes the handler independent
of the platform's mount path, so the same app answers at ``/mcp`` (via a rewrite) or
``/api/mcp``. The transport is hand-rolled in :mod:`hkex_scraper.live_mcp`, so it needs no
SDK, no session state, and no ASGI lifespan hook.

Security: this endpoint is intentionally public and read-only. It applies an Origin
allowlist (MCP's DNS-rebinding mitigation), never caches responses, and rejects every
method except POST. Abuse control is layered at the edge (Vercel WAF rate limiting).
"""

from __future__ import annotations

import json
import os
from typing import List

from fastapi import FastAPI, Request, Response

from hkex_scraper import live_mcp

_DEFAULT_ORIGINS = (
    "https://hkex-listco-updates.ascent-partners.com,http://localhost,http://127.0.0.1"
)


def _allowed_origins() -> List[str]:
    raw = os.environ.get("MCP_ALLOWED_ORIGINS", _DEFAULT_ORIGINS)
    return [origin.strip().rstrip("/") for origin in raw.split(",") if origin.strip()]


def _render(result: "live_mcp.RpcResponse") -> Response:
    if result.body is None:
        return Response(status_code=result.status, headers=result.headers)
    return Response(
        content=json.dumps(result.body, ensure_ascii=False),
        status_code=result.status,
        headers=result.headers,
    )


app = FastAPI(title=live_mcp.SERVER_NAME, docs_url=None, redoc_url=None, openapi_url=None)


@app.get("/healthz")
async def healthz() -> Response:
    """Unauthenticated liveness probe (used by uptime checks)."""
    return Response(content='{"ok":true}', media_type="application/json")


@app.post("/{full_path:path}")
async def mcp_post(full_path: str, request: Request) -> Response:
    origin = request.headers.get("origin")
    if origin and origin.rstrip("/") not in _allowed_origins():
        return Response(content="forbidden origin", status_code=403, media_type="text/plain")
    try:
        message = json.loads(await request.body())
    except Exception:  # noqa: BLE001 - malformed body becomes a JSON-RPC parse error
        return _render(live_mcp.parse_error_response())
    return _render(live_mcp.handle_jsonrpc(message))


@app.api_route("/{full_path:path}", methods=["GET", "DELETE", "PUT", "PATCH", "OPTIONS"])
async def method_not_allowed(full_path: str) -> Response:
    """Stateless Streamable HTTP is POST-only: everything else is 405."""
    return Response(status_code=405, headers={"Allow": "POST"})
