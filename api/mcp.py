"""Vercel WSGI entry point for the live HKEx MCP gateway (legacy Streamable HTTP).

Standard library only: Vercel bundles a Python function from the project's dependency
graph (``uv.lock``), not from ``requirements.txt``, so a web-framework dependency would
not be installed. ``POST`` carries MCP JSON-RPC; ``OPTIONS`` answers the CORS preflight,
and any other method gets an explanatory 405. The transport itself lives in
:mod:`hkex_scraper.live_mcp`.

Security: this endpoint is intentionally public and read-only. It applies an Origin
allowlist (MCP's DNS-rebinding mitigation) and never caches responses.
"""

from __future__ import annotations

import json
import os
from typing import Optional

from hkex_scraper import live_mcp

_DEFAULT_ORIGINS = (
    "https://hkex-listco-updates.ascent-partners.com,http://localhost,http://127.0.0.1"
)
_DOCS_URL = "https://hkex-listco-updates.ascent-partners.com/live-mcp/"
_CORS_ALLOW_HEADERS = "content-type, accept, mcp-protocol-version, mcp-session-id, authorization"

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


def _origin_rejected(origin: Optional[str], allowed: list) -> bool:
    """Reject only non-allowlisted *web* origins.

    The allowlist is MCP's DNS-rebinding mitigation, which guards against a web page
    driving the endpoint. Desktop/Electron MCP clients send no ``Origin``, ``null``, or a
    ``file://`` origin; those are not browser cross-site requests, so they are allowed.
    """
    if not origin:
        return False
    if origin == "null" or origin.startswith("file:"):
        return False
    return origin.rstrip("/") not in allowed


def _cors_headers(origin: Optional[str], allowed: list) -> list:
    headers = [("Vary", "Origin")]
    if origin and origin.rstrip("/") in allowed:
        headers.append(("Access-Control-Allow-Origin", origin))
    return headers


def _send(
    start_response, status: str, body: bytes, content_type: Optional[str], extra: list
) -> list:
    headers = list(extra)
    if content_type:
        headers.append(("Content-Type", content_type))
    headers.append(("Cache-Control", "no-store"))
    headers.append(("Content-Length", str(len(body))))
    start_response(status, headers)
    return [body]


def _respond(start_response, result: "live_mcp.RpcResponse", extra_headers: list) -> list:
    headers = list(extra_headers) + list(result.headers.items())
    status = f"{result.status} {_REASONS.get(result.status, 'OK')}"
    if result.body is None:
        start_response(status, headers)
        return [b""]
    payload = json.dumps(result.body, ensure_ascii=False).encode("utf-8")
    headers.append(("Content-Length", str(len(payload))))
    start_response(status, headers)
    return [payload]


def _method_not_allowed_body(environ: dict) -> tuple:
    """Return ``(content_type, body)`` for a non-POST request.

    Browsers (``Accept: text/html``) get a short page; other clients get JSON.
    """
    if "text/html" in (environ.get("HTTP_ACCEPT") or ""):
        path = environ.get("PATH_INFO") or "/api/mcp"
        html = (
            '<!doctype html><html lang="en"><head><meta charset="utf-8">'
            "<title>HKEx live MCP gateway</title></head><body>"
            "<h1>HKEx live MCP gateway</h1>"
            "<p>This address is a <strong>Model Context Protocol</strong> endpoint. "
            f"Send <code>POST {path}</code> requests with JSON-RPC 2.0; a browser "
            "<code>GET</code> is not supported (HTTP 405).</p>"
            f'<p>See the <a href="{_DOCS_URL}">documentation</a> for tools and client setup.</p>'
            "</body></html>"
        )
        return "text/html; charset=utf-8", html.encode("utf-8")
    body = json.dumps(
        {
            "jsonrpc": "2.0",
            "error": {
                "code": -32600,
                "message": (
                    "Method Not Allowed: this endpoint accepts POST (MCP JSON-RPC 2.0) only"
                ),
            },
            "id": None,
        }
    ).encode("utf-8")
    return "application/json", body


def app(environ: dict, start_response):
    """WSGI application: MCP JSON-RPC over POST; CORS preflight; 405 otherwise."""
    method = (environ.get("REQUEST_METHOD") or "GET").upper()
    origin = environ.get("HTTP_ORIGIN")
    allowed = _allowed_origins()
    cors = _cors_headers(origin, allowed)
    forbidden = _origin_rejected(origin, allowed)

    if method == "OPTIONS":
        if forbidden:
            return _send(
                start_response,
                "403 Forbidden",
                b"forbidden origin",
                "text/plain; charset=utf-8",
                cors,
            )
        preflight = cors + [
            ("Access-Control-Allow-Methods", "POST, OPTIONS"),
            ("Access-Control-Allow-Headers", _CORS_ALLOW_HEADERS),
            ("Access-Control-Max-Age", "600"),
            ("Allow", "POST, OPTIONS"),
        ]
        return _send(start_response, "204 No Content", b"", None, preflight)

    if method != "POST":
        content_type, body = _method_not_allowed_body(environ)
        return _send(
            start_response, "405 Method Not Allowed", body, content_type, cors + [("Allow", "POST")]
        )

    if forbidden:
        return _send(
            start_response, "403 Forbidden", b"forbidden origin", "text/plain; charset=utf-8", cors
        )

    try:
        message = json.loads(_read_body(environ))
    except Exception:  # noqa: BLE001 - malformed body becomes a JSON-RPC parse error
        return _respond(start_response, live_mcp.parse_error_response(), cors)
    return _respond(start_response, live_mcp.handle_jsonrpc(message), cors)
