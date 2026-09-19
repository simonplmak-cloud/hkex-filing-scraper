"""Live HKEx MCP gateway for serverless deployment (Streamable HTTP, stateless).

Unlike :mod:`hkex_scraper.mcp_server` — a **stdio** server that reads a *stored* corpus
from a database sink — this module serves **live** results by calling the HKEx JSON API on
every request. It holds no state, writes nothing, and needs no database, which is what makes
it safe to run as a public serverless function.

The tool logic is transport-agnostic: the ``_tool_*`` helpers and :func:`handle_tool` can be
tested offline and driven by either the MCP SDK's ASGI app (:func:`build_asgi`) or a
hand-rolled JSON-RPC shim. The ``mcp`` SDK is imported lazily behind the ``mcp`` extra, so
``import hkex_scraper`` never requires it.

Security posture (this endpoint is intentionally public and unauthenticated):

* **Read-only by construction** — three read tools, no arbitrary URL/SQL surface.
* **SSRF allowlist** — ``get_filing`` fetches only HKEx document hosts.
* **Bounded** — a hard result cap, a bounded date window, and text/table truncation so a
  response can never approach the platform body limit.
* **Stateless** — a fresh HTTP session per request; no module-global mutable state.

Nothing here may write to stdout when run over stdio: stdout carries the MCP protocol.
"""

from __future__ import annotations

import functools
import json
from datetime import datetime
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Tuple
from urllib.parse import urlparse

from . import __version__, api, http
from .config import HKEX_BASE_URL

try:  # pragma: no cover - exercised via the mcp extra
    from mcp.server.fastmcp import FastMCP
    from mcp.server.fastmcp.exceptions import ToolError
    from mcp.types import ToolAnnotations

    _MCP_AVAILABLE = True
except Exception:  # pragma: no cover - depends on the environment
    FastMCP = None  # type: ignore[assignment]
    ToolError = None  # type: ignore[assignment]
    ToolAnnotations = None  # type: ignore[assignment]
    _MCP_AVAILABLE = False


SERVER_NAME = "hkex-filing-scraper-live"
TRANSPORT = "streamable-http"
STREAMABLE_HTTP_PATH = "/mcp"

# Legacy MCP protocol versions whose Streamable HTTP semantics this transport implements.
PROTOCOL_VERSION = "2024-11-05"
SUPPORTED_PROTOCOL_VERSIONS = ("2024-11-05", "2025-06-18", "2025-11-25")

# Hard server-side caps. The model can raise none of these.
DEFAULT_MAX_RESULTS = 50
MAX_MAX_RESULTS = 200
MAX_WINDOW_DAYS = 31
MAX_TEXT_CHARS = 300_000
MAX_TABLES = 30
FETCH_TIMEOUT_SECONDS = 60

DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y")

# Document downloads are restricted to these hosts (and their subdomains).
ALLOWED_DOC_HOSTS = ("hkexnews.hk",)


def _allowed_doc_hosts() -> Tuple[str, ...]:
    hosts: set[str] = set(ALLOWED_DOC_HOSTS)
    host = (urlparse(HKEX_BASE_URL).hostname or "").lower()
    if host:
        hosts.add(host)
    return tuple(sorted(hosts))


INSTRUCTIONS = (
    "Live, read-only access to HKEx (Hong Kong Stock Exchange) regulatory filings. Every "
    "call fetches fresh data from the HKEx website; nothing is stored. Use search_filings "
    "with a date window of at most 31 days to find filings, then get_filing on a returned "
    "link to download and extract its text and tables. This server never writes and never "
    "fetches arbitrary URLs."
)


class McpError(Exception):
    """An expected, user-actionable tool failure (mapped to ``isError``)."""


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def _clamp(value: int, low: int, high: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return low
    return max(low, min(high, number))


def _parse_date(value: str, field: str):
    text = str(value or "").strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise McpError(f"{field} must be YYYY-MM-DD or DD/MM/YYYY; got '{text}'") from None


def _normalise_code(value: Any) -> str:
    return str(value or "").strip().lstrip("0").upper()


def is_allowed_document_url(url: str) -> bool:
    """True only for http(s) URLs on an allowlisted HKEx document host.

    This is the SSRF guard: ``get_filing`` must never fetch an arbitrary URL.
    """
    parsed = urlparse(str(url or "").strip())
    if parsed.scheme not in ("http", "https"):
        return False
    host = (parsed.hostname or "").lower()
    if not host:
        return False
    return any(host == allowed or host.endswith("." + allowed) for allowed in _allowed_doc_hosts())


def _truncate_text(text: str, limit: int) -> Tuple[str, bool]:
    if len(text) <= limit:
        return text, False
    return text[:limit], True


def _cap_tables(tables: List[Any], limit: int) -> Tuple[List[Any], int]:
    if len(tables) <= limit:
        return tables, 0
    return tables[:limit], len(tables) - limit


# ---------------------------------------------------------------------------
# Tool bodies (transport-agnostic)
# ---------------------------------------------------------------------------
def _tool_get_server_info() -> Dict[str, Any]:
    return {
        "name": SERVER_NAME,
        "version": __version__,
        "transport": TRANSPORT,
        "live": True,
        "storage": "none",
        "read_only": True,
        "tools": ["get_server_info", "search_filings", "get_filing"],
        "limits": {
            "max_results": MAX_MAX_RESULTS,
            "max_window_days": MAX_WINDOW_DAYS,
            "max_text_chars": MAX_TEXT_CHARS,
            "max_tables": MAX_TABLES,
        },
    }


def _tool_search_filings(
    from_date: str,
    to_date: str,
    stock_code: str = "",
    max_results: int = DEFAULT_MAX_RESULTS,
) -> Dict[str, Any]:
    start = _parse_date(from_date, "from_date")
    end = _parse_date(to_date, "to_date")
    if end < start:
        raise McpError("to_date must be on or after from_date")
    if (end - start).days > MAX_WINDOW_DAYS:
        raise McpError(f"date window too wide: request at most {MAX_WINDOW_DAYS} days per call")

    limit = _clamp(max_results, 1, MAX_MAX_RESULTS)
    session = http.make_session()
    try:
        records, total = api.fetch_chunk_via_api(
            session, start.strftime("%Y%m%d"), end.strftime("%Y%m%d"), limit
        )
    except Exception as exc:  # noqa: BLE001 - surfaced as an actionable tool error
        raise McpError(f"HKEx request failed: {type(exc).__name__}") from None

    if stock_code:
        needle = _normalise_code(stock_code)
        records = [r for r in records if _normalise_code(r.get("stockCode")) == needle]

    return {
        "from": start.isoformat(),
        "to": end.isoformat(),
        "count": len(records),
        "total_reported": total,
        "filings": records,
    }


def _tool_get_filing(link: str, extract: bool = True) -> Dict[str, Any]:
    if not is_allowed_document_url(link):
        raise McpError("link must be an HKEx document URL under www1.hkexnews.hk")

    session = http.make_session()
    try:
        response = session.get(link, timeout=FETCH_TIMEOUT_SECONDS)
        response.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        raise McpError(f"document download failed: {type(exc).__name__}") from None

    raw = response.content
    result: Dict[str, Any] = {
        "link": link,
        "size_bytes": len(raw),
        "content_type": response.headers.get("Content-Type", ""),
    }
    if not extract:
        return result

    from .extractor import extract_content_with_tables

    text, tables = extract_content_with_tables(raw, link)
    text, truncated = _truncate_text(text, MAX_TEXT_CHARS)
    tables, omitted = _cap_tables(list(tables), MAX_TABLES)
    result.update(
        {
            "document_text": text,
            "text_length": len(text),
            "text_truncated": truncated,
            "tables": tables,
            "tables_omitted": omitted,
        }
    )
    return result


_HANDLERS: Dict[str, Callable[..., Dict[str, Any]]] = {
    "get_server_info": _tool_get_server_info,
    "search_filings": _tool_search_filings,
    "get_filing": _tool_get_filing,
}


def handle_tool(name: str, arguments: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Dispatch a tool call by name. Transport-agnostic entry point."""
    handler = _HANDLERS.get(str(name))
    if handler is None:
        raise McpError(f"unknown tool: {name}")
    return handler(**(arguments or {}))


# ---------------------------------------------------------------------------
# MCP-facing tools (docstrings become tool descriptions)
# ---------------------------------------------------------------------------
def _as_tool(fn: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except McpError as exc:
            raise ToolError(str(exc)) from None  # type: ignore[misc]

    return wrapper


@_as_tool
def get_server_info() -> Dict[str, Any]:
    """Report this live gateway's version, transport, and hard limits. Reads nothing."""
    return _tool_get_server_info()


@_as_tool
def search_filings(
    from_date: str,
    to_date: str,
    stock_code: str = "",
    max_results: int = DEFAULT_MAX_RESULTS,
) -> Dict[str, Any]:
    """Search live HKEx filings in a date window (at most 31 days).

    ``from_date``/``to_date`` accept YYYY-MM-DD or DD/MM/YYYY. When ``stock_code`` is set,
    results are filtered to that code (e.g. ``01461`` or ``1461``). ``max_results`` caps the
    number of filings fetched and returned (hard cap 200). Returns the parsed filings plus
    the HKEx-reported total for the window.
    """
    return _tool_search_filings(from_date, to_date, stock_code, max_results)


@_as_tool
def get_filing(link: str, extract: bool = True) -> Dict[str, Any]:
    """Download one HKEx document by its URL and extract its text and tables.

    ``link`` must be an HKEx document URL (host ``www1.hkexnews.hk``); any other host is
    rejected. With ``extract=True`` the response includes extracted ``document_text``
    (truncated) and up to 30 ``tables``; set ``extract=False`` for size/content-type only.
    """
    return _tool_get_filing(link, extract)


TOOLS: List[Callable[..., Any]] = [get_server_info, search_filings, get_filing]


# Explicit JSON Schemas for the three tools, used by the transport-agnostic shim and tests.
TOOL_SCHEMAS: List[Dict[str, Any]] = [
    {
        "name": "get_server_info",
        "description": get_server_info.__doc__ or "",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "search_filings",
        "description": search_filings.__doc__ or "",
        "inputSchema": {
            "type": "object",
            "properties": {
                "from_date": {"type": "string", "description": "YYYY-MM-DD or DD/MM/YYYY"},
                "to_date": {"type": "string", "description": "YYYY-MM-DD or DD/MM/YYYY"},
                "stock_code": {"type": "string", "description": "Optional HKEx stock code"},
                "max_results": {"type": "integer", "minimum": 1, "maximum": MAX_MAX_RESULTS},
            },
            "required": ["from_date", "to_date"],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_filing",
        "description": get_filing.__doc__ or "",
        "inputSchema": {
            "type": "object",
            "properties": {
                "link": {"type": "string", "description": "HKEx document URL"},
                "extract": {"type": "boolean", "description": "Extract text and tables"},
            },
            "required": ["link"],
            "additionalProperties": False,
        },
    },
]


# ---------------------------------------------------------------------------
# Legacy Streamable HTTP JSON-RPC (hand-rolled, lifespan-free)
#
# The mcp SDK's Streamable HTTP server needs its ASGI lifespan to start a task group,
# which serverless platforms do not reliably run. This transport implements the same
# legacy wire format directly, so it works as a plain request handler.
# ---------------------------------------------------------------------------
JSON_HEADERS = {"Content-Type": "application/json", "Cache-Control": "no-store"}
JSONRPC_VERSION = "2.0"
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601


class RpcResponse(NamedTuple):
    """A JSON-RPC HTTP response: status, headers, and an optional JSON body."""

    status: int
    headers: Dict[str, str]
    body: Any


def _jsonrpc_ok(message_id: Any, result: Any) -> Dict[str, Any]:
    return {"jsonrpc": JSONRPC_VERSION, "id": message_id, "result": result}


def _jsonrpc_error(message_id: Any, code: int, message: str) -> Dict[str, Any]:
    return {
        "jsonrpc": JSONRPC_VERSION,
        "id": message_id,
        "error": {"code": code, "message": message},
    }


def server_metadata() -> Dict[str, Any]:
    """The ``serverInfo`` block shared by ``initialize`` and ``get_server_info``."""
    return {"name": SERVER_NAME, "version": __version__}


def _capabilities() -> Dict[str, Any]:
    return {"tools": {"listChanged": False}}


def _negotiate_protocol(requested: Any) -> str:
    if isinstance(requested, str) and requested in SUPPORTED_PROTOCOL_VERSIONS:
        return requested
    return PROTOCOL_VERSION


def _tool_result(value: Any) -> Dict[str, Any]:
    text = json.dumps(value, indent=2, ensure_ascii=False, default=str)
    return {
        "content": [{"type": "text", "text": text}],
        "structuredContent": {"result": value},
        "isError": False,
    }


def _tool_error(message: str) -> Dict[str, Any]:
    return {"content": [{"type": "text", "text": message}], "isError": True}


def _handle_message(message: Any) -> Optional[Dict[str, Any]]:
    """Return a JSON-RPC response object, or ``None`` for a notification."""
    if not isinstance(message, dict) or message.get("jsonrpc") != JSONRPC_VERSION:
        return _jsonrpc_error(None, INVALID_REQUEST, "Invalid Request")
    method = message.get("method")
    message_id = message.get("id")
    if not isinstance(method, str):
        return _jsonrpc_error(message_id, INVALID_REQUEST, "Invalid Request")
    is_notification = message_id is None

    if method == "initialize":
        params = message.get("params") or {}
        result = {
            "protocolVersion": _negotiate_protocol(params.get("protocolVersion")),
            "capabilities": _capabilities(),
            "serverInfo": server_metadata(),
            "instructions": INSTRUCTIONS,
        }
        return _jsonrpc_ok(message_id, result)
    if method == "ping":
        return None if is_notification else _jsonrpc_ok(message_id, {})
    if method == "tools/list":
        return None if is_notification else _jsonrpc_ok(message_id, {"tools": TOOL_SCHEMAS})
    if method == "tools/call":
        if is_notification:
            return None
        params = message.get("params") or {}
        arguments = params.get("arguments") or {}
        try:
            value = handle_tool(
                str(params.get("name")), arguments if isinstance(arguments, dict) else {}
            )
        except McpError as exc:
            return _jsonrpc_ok(message_id, _tool_error(str(exc)))
        except Exception as exc:  # noqa: BLE001 - never leak a traceback
            return _jsonrpc_ok(message_id, _tool_error(f"tool failed: {type(exc).__name__}"))
        return _jsonrpc_ok(message_id, _tool_result(value))
    if method.startswith("notifications/"):
        return None
    return _jsonrpc_error(message_id, METHOD_NOT_FOUND, f"Method not found: {method}")


def handle_jsonrpc(message: Any) -> RpcResponse:
    """Handle one MCP JSON-RPC message (or a batch) over legacy Streamable HTTP.

    Notifications return ``202`` with no body; requests return ``200`` with JSON. This is
    transport-only, so it never needs the SDK, a session, or a lifespan hook.
    """
    if isinstance(message, list):
        responses = [r for r in (_handle_message(item) for item in message) if r is not None]
        if not responses:
            return RpcResponse(202, {}, None)
        return RpcResponse(200, dict(JSON_HEADERS), responses)
    response = _handle_message(message)
    if response is None:
        return RpcResponse(202, {}, None)
    return RpcResponse(200, dict(JSON_HEADERS), response)


def parse_error_response() -> RpcResponse:
    """The response for a body that is not valid JSON."""
    return RpcResponse(200, dict(JSON_HEADERS), _jsonrpc_error(None, PARSE_ERROR, "Parse error"))


# ---------------------------------------------------------------------------
# Server assembly
# ---------------------------------------------------------------------------
def build_server() -> Any:
    """Build the FastMCP server with the three live, read-only tools registered."""
    if not _MCP_AVAILABLE:
        raise RuntimeError(
            'MCP support is not installed; install with: pip install "hkex-filing-scraper[mcp]"'
        )
    annotations = ToolAnnotations(  # type: ignore[misc]
        readOnlyHint=True, destructiveHint=False, openWorldHint=True
    )
    server = FastMCP(  # type: ignore[misc]
        SERVER_NAME,
        instructions=INSTRUCTIONS,
        stateless_http=True,
        json_response=True,
        streamable_http_path=STREAMABLE_HTTP_PATH,
    )
    for tool in TOOLS:
        server.add_tool(tool, annotations=annotations)
    return server


def build_asgi():
    """Return the stateless Streamable HTTP ASGI app for serverless hosting.

    ``stateless_http`` and ``json_response`` are set on the server, so every request gets a
    fresh transport and a single JSON response (no session map, no SSE stream).
    """
    return build_server().streamable_http_app()
