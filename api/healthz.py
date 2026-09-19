"""Unauthenticated liveness probe for the live MCP deployment (standard library only)."""

from __future__ import annotations


def app(environ: dict, start_response):
    start_response(
        "200 OK",
        [("Content-Type", "application/json"), ("Cache-Control", "no-store")],
    )
    return [b'{"ok":true}']
