"""Tests for the Vercel WSGI entry point (:mod:`api.mcp`).

The module lives outside the ``src/`` package, so it is loaded from its path.
"""

from __future__ import annotations

import importlib.util
import io
import json
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location("vercel_api_mcp", _ROOT / "api" / "mcp.py")
assert _spec is not None and _spec.loader is not None
vercel_mcp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(vercel_mcp)

ALLOWED = "https://hkex-listco-updates.ascent-partners.com"


def call(method="POST", body=b"", origin=None, accept=None):
    environ = {
        "REQUEST_METHOD": method,
        "PATH_INFO": "/api/mcp",
        "wsgi.input": io.BytesIO(body),
        "CONTENT_LENGTH": str(len(body)),
    }
    if origin is not None:
        environ["HTTP_ORIGIN"] = origin
    if accept is not None:
        environ["HTTP_ACCEPT"] = accept
    captured = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = dict(headers)
        captured["header_map"] = {key.lower(): value for key, value in headers}

    payload = b"".join(vercel_mcp.app(environ, start_response))
    return captured, payload


def rpc(message) -> bytes:
    return json.dumps(message).encode("utf-8")


# --- non-POST -----------------------------------------------------------------


def test_browser_get_returns_405_html():
    captured, payload = call(method="GET", accept="text/html")
    assert captured["status"].startswith("405")
    assert captured["header_map"]["content-type"].startswith("text/html")
    assert captured["header_map"]["allow"] == "POST"
    assert captured["header_map"]["cache-control"] == "no-store"
    assert "MCP" in payload.decode()


def test_api_get_returns_405_json():
    captured, payload = call(method="GET")
    assert captured["status"].startswith("405")
    assert captured["header_map"]["content-type"] == "application/json"
    assert captured["header_map"]["allow"] == "POST"
    body = json.loads(payload)
    assert body["jsonrpc"] == "2.0"
    assert body["error"]["code"] == -32600


def test_head_is_405():
    captured, _ = call(method="HEAD")
    assert captured["status"].startswith("405")


# --- CORS preflight -----------------------------------------------------------


def test_options_without_origin_returns_204_preflight():
    captured, payload = call(method="OPTIONS")
    assert captured["status"].startswith("204")
    assert payload == b""
    assert "POST" in captured["header_map"]["access-control-allow-methods"]
    assert "content-type" in captured["header_map"]["access-control-allow-headers"]
    assert captured["header_map"]["vary"] == "Origin"


def test_options_with_allowed_origin_echoes_it():
    captured, _ = call(method="OPTIONS", origin=ALLOWED)
    assert captured["status"].startswith("204")
    assert captured["header_map"]["access-control-allow-origin"] == ALLOWED


def test_options_with_disallowed_origin_is_403():
    captured, _ = call(method="OPTIONS", origin="https://evil.example")
    assert captured["status"].startswith("403")
    assert "access-control-allow-origin" not in captured["header_map"]


# --- POST ---------------------------------------------------------------------


def test_post_initialize_succeeds_and_varies_origin():
    body = rpc(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {"protocolVersion": "2024-11-05", "capabilities": {}},
        }
    )
    captured, payload = call(body=body)
    assert captured["status"].startswith("200")
    assert captured["header_map"]["cache-control"] == "no-store"
    assert captured["header_map"]["vary"] == "Origin"
    result = json.loads(payload)["result"]
    assert result["serverInfo"]["name"] == "hkex-filing-scraper-live"


def test_post_with_allowed_origin_echoes_it():
    body = rpc({"jsonrpc": "2.0", "id": 1, "method": "ping"})
    captured, _ = call(body=body, origin=ALLOWED)
    assert captured["header_map"]["access-control-allow-origin"] == ALLOWED


def test_post_with_disallowed_origin_is_403():
    body = rpc({"jsonrpc": "2.0", "id": 1, "method": "ping"})
    captured, payload = call(body=body, origin="https://evil.example")
    assert captured["status"].startswith("403")
    assert payload == b"forbidden origin"


def test_post_malformed_json_is_parse_error():
    captured, payload = call(body=b"{not json")
    assert captured["status"].startswith("200")
    assert json.loads(payload)["error"]["code"] == -32700


def test_post_notification_is_202_with_no_body():
    body = rpc({"jsonrpc": "2.0", "method": "notifications/initialized"})
    captured, payload = call(body=body)
    assert captured["status"].startswith("202")
    assert payload == b""
