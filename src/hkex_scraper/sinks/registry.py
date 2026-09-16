"""Sink registry: id -> construction metadata, imported lazily.

Importing this module must not import any optional driver. Each factory imports
its sink module (and therefore its driver) only when that sink is requested.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from .base import Sink


class UnknownSinkError(ValueError):
    """Raised when an unknown sink id is requested."""


@dataclass(frozen=True)
class SinkSpec:
    id: str
    label: str
    license: str
    source_available: bool  # OSI-approved open source?
    extra: Optional[str]  # optional-dependency extra name
    factory: Callable[[], Sink]


def _sqlite() -> Sink:
    from .sqlite import SQLiteSink

    return SQLiteSink()


def _postgres() -> Sink:
    from .postgres import PostgresSink

    return PostgresSink()


def _mysql() -> Sink:
    from .mysql import MySQLSink

    return MySQLSink()


def _mariadb() -> Sink:
    from .mysql import MariaDBSink

    return MariaDBSink()


def _surrealdb() -> Sink:
    from .surrealdb import SurrealDBSink

    return SurrealDBSink()


def _duckdb() -> Sink:
    from .duckdb import DuckDBSink

    return DuckDBSink()


def _mongodb() -> Sink:
    from .mongodb import MongoDBSink

    return MongoDBSink()


def _clickhouse() -> Sink:
    from .clickhouse import ClickHouseSink

    return ClickHouseSink()


def _neo4j() -> Sink:
    from .neo4j import Neo4jSink

    return Neo4jSink()


SINKS: Dict[str, SinkSpec] = {
    "postgres": SinkSpec(
        id="postgres",
        label="PostgreSQL",
        license="PostgreSQL License",
        source_available=True,
        extra="postgres",
        factory=_postgres,
    ),
    "mysql": SinkSpec(
        id="mysql",
        label="MySQL",
        license="GPLv2 (Community)",
        source_available=True,
        extra="mysql",
        factory=_mysql,
    ),
    "mariadb": SinkSpec(
        id="mariadb",
        label="MariaDB",
        license="GPLv2",
        source_available=True,
        extra="mysql",
        factory=_mariadb,
    ),
    "sqlite": SinkSpec(
        id="sqlite",
        label="SQLite",
        license="Public domain",
        source_available=True,
        extra=None,
        factory=_sqlite,
    ),
    "surrealdb": SinkSpec(
        id="surrealdb",
        label="SurrealDB",
        license="BSL 1.1 (source-available)",
        source_available=False,
        extra=None,
        factory=_surrealdb,
    ),
    "duckdb": SinkSpec(
        id="duckdb",
        label="DuckDB",
        license="MIT",
        source_available=True,
        extra="duckdb",
        factory=_duckdb,
    ),
    "mongodb": SinkSpec(
        id="mongodb",
        label="MongoDB",
        license="SSPL (source-available)",
        source_available=False,
        extra="mongodb",
        factory=_mongodb,
    ),
    "clickhouse": SinkSpec(
        id="clickhouse",
        label="ClickHouse",
        license="Apache-2.0",
        source_available=True,
        extra="clickhouse",
        factory=_clickhouse,
    ),
    "neo4j": SinkSpec(
        id="neo4j",
        label="Neo4j (Community)",
        license="GPLv3 (Community)",
        source_available=True,
        extra="neo4j",
        factory=_neo4j,
    ),
}

# Order used for help/error text and the documented default suggestion.
DEFAULT_SINK = "postgres"


def known_ids() -> List[str]:
    """Return the sorted list of valid sink ids."""
    return sorted(SINKS)


def spec(sink_id: str) -> SinkSpec:
    """Return the spec for *sink_id*, raising :class:`UnknownSinkError` if absent."""
    try:
        return SINKS[sink_id]
    except KeyError:
        raise UnknownSinkError(
            f"unknown sink '{sink_id}'; valid sinks are: {', '.join(known_ids())}"
        ) from None


def get_sink(sink_id: str) -> Sink:
    """Construct the sink for *sink_id* (importing its driver lazily)."""
    return spec(sink_id).factory()
