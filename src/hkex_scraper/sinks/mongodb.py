"""MongoDB sink — optional ``pymongo`` driver, document model.

Collections:

- ``exchange_filing`` — one document per filing, ``_id = filing_id``.
- ``scrape_coverage`` — one document per chunk, ``_id = "<from>|<to>|<run_id>"``.
- ``has_filing`` / ``references_filing`` — one document per edge.

Upserts use ``update_one(..., upsert=True)`` with ``$set`` so a metadata write
never touches ``document_*`` fields.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .. import config
from ..utils import ticker_to_record_id
from .base import (
    EDGE_KINDS,
    ERR_NONE,
    SUFFIX_DRIVER_MISSING,
    SUFFIX_DSN_MISSING,
    SUFFIX_PAYLOAD_ERROR,
    SUFFIX_SCHEMA_ERROR,
    SUFFIX_WRITE_ERROR,
    Sink,
    SinkCapabilities,
    code,
    redact,
)

try:  # pragma: no cover - exercised via monkeypatching in tests
    import pymongo  # type: ignore
    from pymongo import UpdateOne  # type: ignore
    from pymongo.errors import PyMongoError  # type: ignore

    _PYMONGO_AVAILABLE = True
except Exception:  # pragma: no cover - depends on environment
    pymongo = None  # type: ignore
    UpdateOne = None  # type: ignore
    PyMongoError = Exception  # type: ignore
    _PYMONGO_AVAILABLE = False

FILING_COLLECTION = "exchange_filing"
COVERAGE_COLLECTION = "scrape_coverage"
EDGE_COLLECTIONS = {"has_filing": "has_filing", "references_filing": "references_filing"}

_METADATA_FIELDS = (
    "filing_id",
    "company_ticker",
    "stock_code",
    "stock_name",
    "exchange",
    "filing_type",
    "filing_subtype",
    "filing_category",
    "title",
    "filing_date",
    "document_url",
    "referenced_tickers",
    "source",
    "updated_at",
)

_DOCUMENT_FIELDS = (
    "document_size",
    "document_type",
    "document_hash",
    "document_text",
    "document_text_len",
    "document_tables",
    "document_table_cnt",
    "document_status",
    "document_status_reason",
)

_INDEXES = (
    ("company_ticker",),
    ("stock_code",),
    ("filing_date",),
    ("filing_type",),
    ("document_status",),
)


class MongoDBSink(Sink):
    id = "mongodb"
    capabilities = SinkCapabilities(
        model="document",
        native_upsert=True,
        reads=True,
        edges=True,
        json=True,
        arrays=True,
        transactions=False,
        bulk=True,
    )

    def __init__(self) -> None:
        self._client = None

    # -- lifecycle ---------------------------------------------------------
    def driver_installed(self) -> bool:
        return _PYMONGO_AVAILABLE

    def configured(self) -> bool:
        return bool(config.MONGODB_URI and config.MONGODB_DATABASE)

    def available(self) -> bool:
        return self.driver_installed() and self.configured()

    def unavailable_reason(self) -> str:
        if not _PYMONGO_AVAILABLE:
            return 'MongoDB sink requires pymongo (install with: pip install ".[mongodb]")'
        if not config.MONGODB_URI:
            return "MongoDB sink requires MONGODB_URI"
        if not config.MONGODB_DATABASE:
            return "MongoDB sink requires MONGODB_DATABASE"
        return "MongoDB sink is unavailable"

    def _database(self) -> Tuple[Any, str]:
        if not _PYMONGO_AVAILABLE or pymongo is None:
            return None, code(self.id, SUFFIX_DRIVER_MISSING)
        if not self.configured():
            return None, code(self.id, SUFFIX_DSN_MISSING)
        if self._client is None:
            try:
                self._client = pymongo.MongoClient(
                    config.MONGODB_URI, serverSelectionTimeoutMS=5000
                )
            except Exception as exc:  # noqa: BLE001
                return None, redact(str(exc)) or code(self.id, SUFFIX_WRITE_ERROR)
        return self._client[config.MONGODB_DATABASE], ERR_NONE

    def ensure_schema(self) -> Tuple[bool, str]:
        db, err = self._database()
        if err:
            return False, err
        try:
            for columns in _INDEXES:
                db[FILING_COLLECTION].create_index(list(columns))
            db[COVERAGE_COLLECTION].create_index("run_id")
            db[EDGE_COLLECTIONS["has_filing"]].create_index("filing_id")
            db[EDGE_COLLECTIONS["references_filing"]].create_index("company_id")
            return True, ERR_NONE
        except Exception as exc:  # noqa: BLE001
            return False, redact(str(exc)) or code(self.id, SUFFIX_SCHEMA_ERROR)

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:  # pragma: no cover - best effort
                pass
            self._client = None

    # -- writes ------------------------------------------------------------
    def upsert_filings(self, records: List[Dict[str, Any]]) -> Tuple[int, str]:
        if not records:
            return 0, ERR_NONE
        db, err = self._database()
        if err:
            return 0, err
        try:
            operations = [
                UpdateOne(
                    {"_id": record["filing_id"]},
                    {"$set": {field: record.get(field) for field in _METADATA_FIELDS}},
                    upsert=True,
                )
                for record in records
            ]
            db[FILING_COLLECTION].bulk_write(operations, ordered=False)
            return len(records), ERR_NONE
        except Exception as exc:  # noqa: BLE001
            return 0, redact(str(exc)) or code(self.id, SUFFIX_WRITE_ERROR)

    def upsert_document(self, filing_id: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
        db, err = self._database()
        if err:
            return False, err
        try:
            result = db[FILING_COLLECTION].update_one(
                {"_id": filing_id},
                {
                    "$set": {
                        **{field: payload.get(field) for field in _DOCUMENT_FIELDS},
                        "updated_at": datetime.now(timezone.utc),
                    }
                },
                upsert=False,
            )
            if result.matched_count == 0:
                return False, code(self.id, SUFFIX_WRITE_ERROR)
            return True, ERR_NONE
        except Exception as exc:  # noqa: BLE001
            return False, redact(str(exc)) or code(self.id, SUFFIX_WRITE_ERROR)

    def mark_status(self, filing_id: str, status: str, reason: str = "") -> Tuple[bool, str]:
        db, err = self._database()
        if err:
            return False, err
        try:
            result = db[FILING_COLLECTION].update_one(
                {"_id": filing_id},
                {
                    "$set": {
                        "document_status": status,
                        "document_status_reason": reason or "",
                        "updated_at": datetime.now(timezone.utc),
                    }
                },
                upsert=False,
            )
            if result.matched_count == 0:
                return False, code(self.id, SUFFIX_WRITE_ERROR)
            return True, ERR_NONE
        except Exception as exc:  # noqa: BLE001
            return False, redact(str(exc)) or code(self.id, SUFFIX_WRITE_ERROR)

    def insert_coverage(self, chunk: Dict[str, Any]) -> Tuple[bool, str]:
        db, err = self._database()
        if err:
            return False, err
        key = f"{chunk.get('chunk_from')}|{chunk.get('chunk_to')}|{chunk.get('run_id', '')}"
        try:
            db[COVERAGE_COLLECTION].update_one(
                {"_id": key},
                {
                    "$set": {
                        "chunk_from": chunk.get("chunk_from"),
                        "chunk_to": chunk.get("chunk_to"),
                        "api_count": chunk.get("api_count", 0),
                        "ingested_count": chunk.get("ingested_count", 0),
                        "unique_count": chunk.get("unique_count", 0),
                        "run_id": chunk.get("run_id", ""),
                        "timestamp": chunk.get("timestamp") or datetime.now(timezone.utc),
                    }
                },
                upsert=True,
            )
            return True, ERR_NONE
        except Exception as exc:  # noqa: BLE001
            return False, redact(str(exc)) or code(self.id, SUFFIX_WRITE_ERROR)

    def upsert_edges(self, edges: List[Dict[str, Any]], kind: str) -> Tuple[int, str]:
        if kind not in EDGE_KINDS:
            return 0, code(self.id, SUFFIX_PAYLOAD_ERROR)
        if not edges:
            return 0, ERR_NONE
        db, err = self._database()
        if err:
            return 0, err
        try:
            operations = []
            for edge in edges:
                company_id = edge.get("company_id") or ticker_to_record_id(
                    edge.get("company_ticker", "")
                )
                filing_id = edge.get("filing_id", "")
                if not company_id or not filing_id:
                    continue
                if kind == "has_filing":
                    key = f"{company_id}|{filing_id}"
                    document = {
                        "company_id": company_id,
                        "filing_id": filing_id,
                        "created_at": datetime.now(timezone.utc),
                    }
                else:
                    key = f"{filing_id}|{company_id}"
                    document = {
                        "filing_id": filing_id,
                        "company_id": company_id,
                        "source": edge.get("source", "title_extraction"),
                        "created_at": datetime.now(timezone.utc),
                    }
                operations.append(UpdateOne({"_id": key}, {"$setOnInsert": document}, upsert=True))
            result = db[EDGE_COLLECTIONS[kind]].bulk_write(operations, ordered=False)
            return result.upserted_count, ERR_NONE
        except Exception as exc:  # noqa: BLE001
            return 0, redact(str(exc)) or code(self.id, SUFFIX_WRITE_ERROR)

    # -- reads -------------------------------------------------------------
    def count_filings(self) -> Tuple[int, str]:
        db, err = self._database()
        if err:
            return 0, err
        try:
            return db[FILING_COLLECTION].count_documents({}), ERR_NONE
        except Exception as exc:  # noqa: BLE001
            return 0, redact(str(exc)) or code(self.id, SUFFIX_WRITE_ERROR)

    def count_edges(self, kind: str) -> Tuple[int, str]:
        if kind not in EDGE_KINDS:
            return 0, code(self.id, SUFFIX_PAYLOAD_ERROR)
        db, err = self._database()
        if err:
            return 0, err
        try:
            return db[EDGE_COLLECTIONS[kind]].count_documents({}), ERR_NONE
        except Exception as exc:  # noqa: BLE001
            return 0, redact(str(exc)) or code(self.id, SUFFIX_WRITE_ERROR)

    def count_pending_filings(self) -> Tuple[int, str]:
        db, err = self._database()
        if err:
            return 0, err
        try:
            pending = {"document_status": None, "document_url": {"$nin": [None, ""]}}
            return db[FILING_COLLECTION].count_documents(pending), ERR_NONE
        except Exception as exc:  # noqa: BLE001
            return 0, redact(str(exc)) or code(self.id, SUFFIX_WRITE_ERROR)

    def fetch_pending_filings(self, limit: int) -> Tuple[List[Dict[str, Any]], str]:
        db, err = self._database()
        if err:
            return [], err
        try:
            pending = {"document_status": None, "document_url": {"$nin": [None, ""]}}
            cursor = (
                db[FILING_COLLECTION]
                .find(pending, {"_id": 0, "filing_id": 1, "document_url": 1})
                .sort("filing_date", -1)
                .limit(limit)
            )
            return [dict(row) for row in cursor], ERR_NONE
        except Exception as exc:  # noqa: BLE001
            return [], redact(str(exc)) or code(self.id, SUFFIX_WRITE_ERROR)

    def distinct_company_tickers(self) -> Tuple[List[str], str]:
        db, err = self._database()
        if err:
            return [], err
        try:
            values = db[FILING_COLLECTION].distinct(
                "company_ticker", {"company_ticker": {"$nin": [None, ""]}}
            )
            return [v for v in values if v], ERR_NONE
        except Exception as exc:  # noqa: BLE001
            return [], redact(str(exc)) or code(self.id, SUFFIX_WRITE_ERROR)

    def fetch_filing_ids_by_ticker(self, tickers: List[str]) -> Tuple[List[Dict[str, Any]], str]:
        if not tickers:
            return [], ERR_NONE
        db, err = self._database()
        if err:
            return [], err
        try:
            cursor = db[FILING_COLLECTION].find(
                {"company_ticker": {"$in": list(tickers)}},
                {"_id": 0, "company_ticker": 1, "filing_id": 1},
            )
            return [dict(row) for row in cursor], ERR_NONE
        except Exception as exc:  # noqa: BLE001
            return [], redact(str(exc)) or code(self.id, SUFFIX_WRITE_ERROR)

    def fetch_titles(
        self, ticker_set: Optional[List[str]], offset: int, page_size: int
    ) -> Tuple[List[Dict[str, Any]], str]:
        db, err = self._database()
        if err:
            return [], err
        if ticker_set is not None:
            query: Dict[str, Any] = {"company_ticker": {"$in": list(ticker_set)}}
        else:
            query = {"title": {"$nin": [None, ""]}}
        try:
            cursor = (
                db[FILING_COLLECTION]
                .find(
                    query,
                    {"_id": 0, "filing_id": 1, "title": 1, "stock_code": 1, "company_ticker": 1},
                )
                .sort("filing_id", 1)
                .skip(offset)
                .limit(page_size)
            )
            return [dict(row) for row in cursor], ERR_NONE
        except Exception as exc:  # noqa: BLE001
            return [], redact(str(exc)) or code(self.id, SUFFIX_WRITE_ERROR)

    def fetch_coverage(self) -> Tuple[List[Dict[str, Any]], str]:
        db, err = self._database()
        if err:
            return [], err
        try:
            cursor = (
                db[COVERAGE_COLLECTION]
                .find(
                    {},
                    {
                        "_id": 0,
                        "chunk_from": 1,
                        "chunk_to": 1,
                        "api_count": 1,
                        "ingested_count": 1,
                        "unique_count": 1,
                        "run_id": 1,
                        "timestamp": 1,
                    },
                )
                .sort("chunk_from", -1)
            )
            return [dict(row) for row in cursor], ERR_NONE
        except Exception as exc:  # noqa: BLE001
            return [], redact(str(exc)) or code(self.id, SUFFIX_WRITE_ERROR)
