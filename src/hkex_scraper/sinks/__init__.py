"""Persistence sinks.

Public surface used by the pipeline, graph linker, and CLI:

- :func:`enabled_sinks` — the configured sinks, in ``DATABASE_TARGET`` order.
- :func:`read_sink` — the first configured sink that can serve reads.
- :func:`get_sink` / :func:`has` — construction and membership.
- :func:`close_all` / :func:`reset` — lifecycle and test isolation.
"""

from __future__ import annotations

from typing import List, Optional

from .. import config
from .base import EDGE_KINDS as EDGE_KINDS
from .base import ERR_NONE as ERR_NONE
from .base import Sink as Sink
from .base import SinkCapabilities as SinkCapabilities
from .base import code as code
from .base import redact as redact
from .registry import DEFAULT_SINK as DEFAULT_SINK
from .registry import SINKS as SINKS
from .registry import SinkSpec as SinkSpec
from .registry import UnknownSinkError as UnknownSinkError
from .registry import get_sink as get_sink
from .registry import known_ids as known_ids
from .registry import spec as spec

_enabled: Optional[List[Sink]] = None


def enabled_sinks() -> List[Sink]:
    """Return the configured sinks in order (constructed once per process)."""
    global _enabled
    if _enabled is None:
        _enabled = [get_sink(sink_id) for sink_id in config.sink_ids()]
    return _enabled


def reset() -> None:
    """Forget the constructed sinks (used by tests and by the CLI override)."""
    global _enabled
    _enabled = None


def close_all() -> None:
    """Close every constructed sink. Best effort; never raises."""
    if _enabled is None:
        return
    for sink in _enabled:
        try:
            sink.close()
        except Exception:  # pragma: no cover - best effort
            pass


def has(sink_id: str) -> bool:
    """True when *sink_id* is among the configured sinks."""
    return any(sink.id == sink_id for sink in enabled_sinks())


def read_sink() -> Optional[Sink]:
    """First configured sink whose capabilities include reads."""
    for sink in enabled_sinks():
        if sink.capabilities.reads:
            return sink
    return None


def edge_sinks() -> List[Sink]:
    """Configured sinks that store graph edges."""
    return [sink for sink in enabled_sinks() if sink.capabilities.edges]


__all__ = [
    "Sink",
    "SinkCapabilities",
    "SinkSpec",
    "SINKS",
    "EDGE_KINDS",
    "ERR_NONE",
    "DEFAULT_SINK",
    "UnknownSinkError",
    "code",
    "redact",
    "enabled_sinks",
    "reset",
    "close_all",
    "has",
    "read_sink",
    "edge_sinks",
    "get_sink",
    "known_ids",
    "spec",
]
