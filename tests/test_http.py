"""HTTP resilience: retry policy, pacing, and the Phase 2 download path.

Every test is offline — ``responses`` intercepts at the adapter boundary, so the real
session, retry policy, and response handling all execute.
"""

from __future__ import annotations

import os
import pathlib
import time

import pytest
import responses
from responses.registries import OrderedRegistry

from hkex_scraper import http, pipeline

URL = "https://www1.hkexnews.hk/listedco/listconews/sehk/2026/0211/2026021100854.pdf"


@pytest.fixture(autouse=True)
def _isolated_http():
    """No shared session, no pacing, no backoff sleeps between tests."""
    for name, value in (
        ("RETRY_BACKOFF_FACTOR", 0.0),
        ("RETRY_BACKOFF_JITTER", 0.0),
        ("RETRY_BACKOFF_MAX", 0.0),
    ):
        setattr(http, name, value)
    http.reset_session()
    http.reset_pacing()
    os.environ.pop("REQUEST_DELAY_SECONDS", None)
    yield
    http.reset_session()
    http.reset_pacing()
    os.environ.pop("REQUEST_DELAY_SECONDS", None)


# ---------------------------------------------------------------------------
# Policy shape
# ---------------------------------------------------------------------------


class TestRetryPolicy:
    def test_policy_values(self):
        retry = http.build_retry()
        assert retry.total == 4
        assert retry.backoff_factor == 0.0  # patched in the fixture
        assert retry.respect_retry_after_header is True
        assert sorted(retry.status_forcelist) == [408, 429, 500, 502, 503, 504]
        assert sorted(retry.allowed_methods) == ["GET", "HEAD", "OPTIONS"]
        # POST must never be replayed: a read timeout can follow server-side processing.
        assert "POST" not in retry.allowed_methods

    def test_session_mounts_an_https_adapter(self):
        session = http.make_session()
        adapter = session.get_adapter("https://example.test/")
        assert adapter.max_retries.total == 4

    def test_user_agent_identifies_the_project_and_contact(self):
        agent = http.user_agent()
        assert agent.startswith("hkex-filing-scraper/")
        assert http.REPO_URL in agent

    def test_shared_session_is_a_singleton(self):
        first = http.get_session()
        assert http.get_session() is first
        http.reset_session()
        assert http.get_session() is not first


# ---------------------------------------------------------------------------
# Retry behaviour
# ---------------------------------------------------------------------------


class TestRetryBehaviour:
    @responses.activate(registry=OrderedRegistry)
    def test_retries_transient_status_then_succeeds(self):
        responses.get(URL, status=503)
        responses.get(URL, status=500)
        responses.get(URL, body="ok", status=200)

        response = http.get_session().get(URL, timeout=1)

        assert response.status_code == 200
        assert response.text == "ok"
        assert len(responses.calls) == 3

    @responses.activate(registry=OrderedRegistry)
    def test_honours_retry_after_header(self):
        responses.get(URL, status=429, headers={"Retry-After": "0"})
        responses.get(URL, body="ok", status=200)

        response = http.get_session().get(URL, timeout=1)

        assert response.status_code == 200
        assert len(responses.calls) == 2

    @responses.activate
    def test_does_not_retry_a_permanent_status(self):
        responses.get(URL, status=404)

        response = http.get_session().get(URL, timeout=1)

        assert response.status_code == 404
        assert len(responses.calls) == 1

    @responses.activate(registry=OrderedRegistry)
    def test_gives_up_after_the_retry_budget_and_returns_the_response(self):
        for _ in range(5):  # 1 attempt + 4 retries
            responses.get(URL, status=503)

        response = http.get_session().get(URL, timeout=1)

        assert response.status_code == 503
        assert len(responses.calls) == 5

    @responses.activate
    def test_post_is_never_retried(self):
        responses.post(URL, status=503)

        response = http.get_session().post(URL, timeout=1)

        assert response.status_code == 503
        assert len(responses.calls) == 1


# ---------------------------------------------------------------------------
# Pacing
# ---------------------------------------------------------------------------


class TestPacing:
    def test_pacing_is_disabled_by_default(self):
        assert http.request_delay() == 0.0
        started = time.monotonic()
        http.pace()
        http.pace()
        assert time.monotonic() - started < 0.1

    def test_invalid_delay_falls_back_to_disabled(self, monkeypatch):
        monkeypatch.setenv("REQUEST_DELAY_SECONDS", "not-a-number")
        assert http.request_delay() == 0.0

    def test_pacing_serialises_request_starts(self, monkeypatch):
        monkeypatch.setenv("REQUEST_DELAY_SECONDS", "0.15")
        http.reset_pacing()
        started = time.monotonic()
        http.pace()
        http.pace()
        assert time.monotonic() - started >= 0.12
        assert http.request_delay() == 0.15

    def test_negative_delay_is_clamped(self, monkeypatch):
        monkeypatch.setenv("REQUEST_DELAY_SECONDS", "-5")
        assert http.request_delay() == 0.0


# ---------------------------------------------------------------------------
# Phase 2 download path
# ---------------------------------------------------------------------------


class TestDownloadDocument:
    @responses.activate
    def test_returns_bytes_for_a_supported_document(self):
        responses.get(URL, body=b"%PDF-1.4 content", status=200)
        content, size, reason = pipeline._download_document(URL, "fid1")
        assert content == b"%PDF-1.4 content"
        assert size == len(content)
        assert reason == ""

    @responses.activate
    def test_retries_a_transient_failure(self):
        responses.get(URL, status=503)
        responses.get(URL, body=b"%PDF", status=200)
        content, size, reason = pipeline._download_document(URL, "fid2")
        assert content == b"%PDF"
        assert reason == ""
        assert len(responses.calls) == 2

    @responses.activate
    def test_reports_an_http_error_as_a_skip_reason(self):
        responses.get(URL, status=404)
        content, size, reason = pipeline._download_document(URL, "fid3")
        assert (content, size) == (b"", 0)
        assert reason == "http_404"

    @responses.activate
    def test_rejects_an_oversized_content_length(self, monkeypatch):
        monkeypatch.setattr(pipeline, "MAX_DOWNLOAD_SIZE", 8)
        responses.get(URL, body=b"0123456789", status=200)
        content, size, reason = pipeline._download_document(URL, "fid4")
        assert (content, size) == (b"", 0)
        assert reason == "too_large"

    @responses.activate
    def test_rejects_an_oversized_stream(self, monkeypatch):
        monkeypatch.setattr(pipeline, "MAX_DOWNLOAD_SIZE", 8)
        responses.get(URL, body=b"x" * 4096, status=200)
        _, _, reason = pipeline._download_document(URL, "fid5")
        assert reason == "too_large"

    def test_rejects_an_unsupported_extension_without_a_request(self):
        content, size, reason = pipeline._download_document(
            "https://www1.hkexnews.hk/listedco/listconews/sehk/2026/0211/file.zip", "fid6"
        )
        assert (content, size, reason) == (b"", 0, "unsupported_type")

    def test_rejects_a_missing_url(self):
        assert pipeline._download_document("", "fid7") == (b"", 0, "no_url")


# ---------------------------------------------------------------------------
# Hashing helpers
# ---------------------------------------------------------------------------


class TestHashes:
    def test_filing_id_is_stable_and_sixteen_hex_chars(self):
        filing = {"stockCode": "01461", "date": "2026-02-11", "title": "Announcement"}
        first = pipeline.filing_id_for(filing)
        assert first == pipeline.filing_id_for(dict(filing))
        assert len(first) == 16
        assert all(c in "0123456789abcdef" for c in first)

    def test_md5_is_marked_as_non_security(self):
        # MD5 here is identity/dedup only. `usedforsecurity=False` keeps it working on
        # FIPS-enabled hosts, where the default MD5 is blocked.
        source = pathlib.Path(pipeline.__file__).read_text(encoding="utf-8")
        assert source.count("usedforsecurity=False") == 2
