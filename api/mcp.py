"""Vercel WSGI entry point for the live HKEx MCP gateway (legacy Streamable HTTP).

Standard library only: Vercel bundles a Python function from the project's dependency
graph (``uv.lock``), not from ``requirements.txt``, so a web-framework dependency would
not be installed. POST carries MCP JSON-RPC; every other method is 405. The transport
itself lives in :mod:`hkex_scraper.live_mcp`.

Security: this endpoint is intentionally public and read-only. It applies an Origin
allowlist (MCP's DNS-rebinding mitigation) and never caches responses.
"""

from __future__ import annotations

import json
import os

from hkex_scraper import live_mcp

_DEFAULT_ORIGINS = (
    "https://hkex-listco-updates.ascent-partners.com,http://localhost,http://127.0.0.1"
)

_REASONS = {
    200: "OK",
    202: "Accepted",
    400: "Bad Request",
    403: "Forbidden",
    405: "Method Not Allowed",
    500: "Internal Server Error",
}


def _allowed_origins() -> list:
    raw = os.environ.get("MCP_ALLOWED_ORIGINS", _DEFAULT_ORIGINS)
    return [origin.strip().rstrip("/") for origin in raw.split(",") if origin.strip()]


def _read_body(environ: dict) -> bytes:
    try:
        length = int(environ.get("CONTENT_LENGTH") or 0)
    except (TypeError, ValueError):
        length = 0
    stream = environ.get("wsgi.input")
    if not stream or length <= 0:
        return b""
    return stream.read(length)


def _respond(start_response, result: "live_mcp.RpcResponse") -> list:
    headers = list(result.headers.items())
    status = f"{result.status} {_REASONS.get(result.status, 'OK')}"
    if result.body is None:
        start_response(status, headers)
        return [b""]
    payload = json.dumps(result.body, ensure_ascii=False).encode("utf-8")
    headers.append(("Content-Length", str(len(payload))))
    start_response(status, headers)
    return [payload]


def app(environ: dict, start_response):
    """WSGI application: MCP JSON-RPC over POST; 405 for everything else."""
    if (environ.get("REQUEST_METHOD") or "GET").upper() != "POST":
        start_response("405 Method Not Allowed", [("Allow", "POST")])
        return [b""]

    origin = environ.get("HTTP_ORIGIN")
    if origin and origin.rstrip("/") not in _allowed_origins():
        start_response("403 Forbidden", [("Content-Type", "text/plain")])
        return [b"forbidden origin"]

    try:
        message = json.loads(_read_body(environ))
    except Exception:  # noqa: BLE001 - malformed body becomes a JSON-RPC parse error
        return _respond(start_response, live_mcp.parse_error_response())
    return _respond(start_response, live_mcp.handle_jsonrpc(message))
