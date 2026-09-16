"""Shared test fixtures.

Ordinary tests must not touch the network. A test that genuinely needs it marks itself
``@pytest.mark.network`` and is excluded from the default run.
"""

from __future__ import annotations

import socket
from typing import Any

import pytest

_LOCAL_HOSTS = {"localhost", "::1", "0.0.0.0"}


def _is_local(address: Any) -> bool:
    if isinstance(address, (str, bytes)):
        host = address.decode() if isinstance(address, bytes) else address
    elif isinstance(address, tuple) and address:
        host = address[0]
    else:
        return False
    host = str(host)
    if host in _LOCAL_HOSTS:
        return True
    # Loopback ranges (127.0.0.0/8 and IPv6 ::1) plus unix sockets.
    return host.startswith("127.") or host.startswith("/") or host == ""


@pytest.fixture(autouse=True)
def _no_external_network(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest):
    """Fail fast on unexpected outbound connections.

    Connections to loopback are allowed so the integration suites still work when a service
    is running locally (CI uses 127.0.0.1). Mark a test ``network`` to opt out entirely.
    """
    if request.node.get_closest_marker("network"):
        return
    real_connect = socket.socket.connect

    def guarded(self, address):  # type: ignore[no-untyped-def]
        if _is_local(address):
            return real_connect(self, address)
        raise AssertionError(
            f"Unexpected network access to {address!r}. Mark the test with "
            "@pytest.mark.network, or mock the request (for example with `responses`)."
        )

    monkeypatch.setattr(socket.socket, "connect", guarded)
