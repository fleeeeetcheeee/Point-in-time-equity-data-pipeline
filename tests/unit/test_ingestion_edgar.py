"""
Tests for the EDGAR bulk-dataset downloader.

Why this file exists
--------------------
On 2026-08-11 the configured bulk URL was found to be dead — the SEC had moved
the Financial Statement Data Sets from

    https://www.sec.gov/dera/data/financial-statements/{y}q{q}.zip        (404)

to

    https://www.sec.gov/files/dera/data/financial-statement-data-sets/…  (200)

The whole suite passed anyway, because `ingestion/` had no tests at all: every
existing test starts from a fixture that is *already downloaded*. The pipeline
would have failed on the first quarter of a multi-hour bootstrap run.

These tests mock the HTTP layer with `responses`, so they assert the client's
behaviour — the URL it builds, the header it sends, what it leaves on disk when
a download fails — without touching the network or depending on the SEC being
up. `test_bulk_url_is_not_the_retired_path` is the specific regression guard.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pytest
import responses
from tenacity import wait_none

from tierzero.config import config
from tierzero.ingestion.edgar import EdgarDownloader

USER_AGENT = "Test Runner test@example.com"

# The three files a Financial Statement Data Set zip must contain for
# _is_complete() to consider a quarter downloaded.
REQUIRED_FILES = {"sub.txt", "num.txt", "tag.txt"}


@pytest.fixture(autouse=True)
def no_retry_backoff(monkeypatch):
    """
    Strip tenacity's exponential wait for the duration of a test.

    _download_file retries 5 times with 4→60s backoff. That is correct against
    a flaky SEC but would add ~60s to every failure-path test here.
    """
    monkeypatch.setattr(
        EdgarDownloader._download_file.retry, "wait", wait_none(), raising=False
    )


def _fake_zip_bytes() -> bytes:
    """A minimal but structurally real Financial Statement Data Set zip."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("sub.txt", "adsh\tcik\tname\tform\tfiled\tperiod\n")
        zf.writestr("num.txt", "adsh\ttag\tddate\tqtrs\tuom\tvalue\n")
        zf.writestr("tag.txt", "tag\tversion\tcustom\tabstract\n")
    return buf.getvalue()


def _downloader(tmp_path: Path) -> EdgarDownloader:
    return EdgarDownloader(output_dir=tmp_path, user_agent=USER_AGENT)


# ======================================================================
# The URL itself
# ======================================================================

class TestBulkUrl:

    def test_bulk_url_is_not_the_retired_path(self):
        """
        The pre-2026 path 404s. Nothing may reintroduce it.
        """
        url = config.edgar_bulk_url_pattern.format(year=2024, quarter=1)
        assert "/dera/data/financial-statements/" not in url, (
            "This is the retired SEC path and returns 404 — see the module docstring."
        )

    def test_bulk_url_has_the_expected_shape(self):
        url = config.edgar_bulk_url_pattern.format(year=2024, quarter=1)
        assert url == (
            "https://www.sec.gov/files/dera/data/"
            "financial-statement-data-sets/2024q1.zip"
        )

    def test_downloader_takes_its_url_from_config(self, tmp_path):
        """
        edgar.py used to carry its own copy of the URL, which is how the two
        drifted. config is the single source; the constructor only overrides
        it for tests.
        """
        assert _downloader(tmp_path).bulk_url_pattern == config.edgar_bulk_url_pattern

    def test_url_pattern_is_injectable(self, tmp_path):
        dl = EdgarDownloader(tmp_path, USER_AGENT, bulk_url_pattern="http://x/{year}{quarter}")
        assert dl.bulk_url_pattern == "http://x/{year}{quarter}"


# ======================================================================
# Downloading a quarter
# ======================================================================

class TestDownloadQuarter:

    @responses.activate
    def test_successful_download_extracts_the_three_tsvs(self, tmp_path):
        url = config.edgar_bulk_url_pattern.format(year=2024, quarter=1)
        responses.add(responses.GET, url, body=_fake_zip_bytes(), status=200)

        dest = _downloader(tmp_path).download_quarter(2024, 1)

        assert dest == tmp_path / "2024q1"
        assert REQUIRED_FILES.issubset({f.name for f in dest.iterdir()})

    @responses.activate
    def test_download_zip_is_removed_after_extraction(self, tmp_path):
        url = config.edgar_bulk_url_pattern.format(year=2024, quarter=1)
        responses.add(responses.GET, url, body=_fake_zip_bytes(), status=200)

        dest = _downloader(tmp_path).download_quarter(2024, 1)

        assert not (dest / "_download.zip").exists()

    @responses.activate
    def test_sec_user_agent_is_sent(self, tmp_path):
        """
        Requests without a descriptive User-Agent are blocked outright, so the
        header travelling on the wire is worth asserting, not assuming.
        """
        url = config.edgar_bulk_url_pattern.format(year=2024, quarter=1)
        responses.add(responses.GET, url, body=_fake_zip_bytes(), status=200)

        _downloader(tmp_path).download_quarter(2024, 1)

        assert responses.calls[0].request.headers["User-Agent"] == USER_AGENT

    @responses.activate
    def test_completed_quarter_is_not_redownloaded(self, tmp_path):
        url = config.edgar_bulk_url_pattern.format(year=2024, quarter=1)
        responses.add(responses.GET, url, body=_fake_zip_bytes(), status=200)

        dl = _downloader(tmp_path)
        dl.download_quarter(2024, 1)
        dl.download_quarter(2024, 1)

        assert len(responses.calls) == 1, "Second call should have hit the skip path"

    @responses.activate
    def test_404_raises_rather_than_writing_an_empty_quarter(self, tmp_path):
        """
        The failure mode that started all this. A dead URL must surface as an
        exception, not as a quarter that looks downloaded but has no data.
        """
        url = config.edgar_bulk_url_pattern.format(year=2024, quarter=1)
        responses.add(responses.GET, url, status=404)

        with pytest.raises(Exception):
            _downloader(tmp_path).download_quarter(2024, 1)

        assert not (tmp_path / "2024q1").exists(), (
            "A failed download left a directory behind; a later run would see it"
        )

    @responses.activate
    def test_partial_directory_is_not_treated_as_complete(self, tmp_path):
        """
        A quarter holding only some of the three TSVs must be re-downloaded,
        not skipped.
        """
        stale = tmp_path / "2024q1"
        stale.mkdir()
        (stale / "sub.txt").write_text("adsh\n")

        url = config.edgar_bulk_url_pattern.format(year=2024, quarter=1)
        responses.add(responses.GET, url, body=_fake_zip_bytes(), status=200)

        _downloader(tmp_path).download_quarter(2024, 1)

        assert len(responses.calls) == 1
        assert REQUIRED_FILES.issubset({f.name for f in stale.iterdir()})

    @responses.activate
    def test_transient_failure_is_retried(self, tmp_path):
        url = config.edgar_bulk_url_pattern.format(year=2024, quarter=1)
        responses.add(responses.GET, url, status=503)
        responses.add(responses.GET, url, body=_fake_zip_bytes(), status=200)

        dest = _downloader(tmp_path).download_quarter(2024, 1)

        assert len(responses.calls) == 2
        assert REQUIRED_FILES.issubset({f.name for f in dest.iterdir()})


# ======================================================================
# Quarter enumeration
# ======================================================================

class TestIterQuarters:

    def test_yields_quarters_in_order(self, tmp_path):
        got = list(_downloader(tmp_path).iter_quarters(2024, 1, 2024, 4))
        assert got == [(2024, 1), (2024, 2), (2024, 3), (2024, 4)]

    def test_end_bound_is_inclusive(self, tmp_path):
        got = list(_downloader(tmp_path).iter_quarters(2024, 1, 2024, 1))
        assert got == [(2024, 1)]

    def test_rolls_over_year_boundary(self, tmp_path):
        got = list(_downloader(tmp_path).iter_quarters(2023, 4, 2024, 1))
        assert got == [(2023, 4), (2024, 1)]

    def test_incomplete_quarters_are_excluded(self, tmp_path):
        """
        The SEC publishes a quarter ~45 days after it ends. Asking for quarters
        far in the future must yield nothing rather than 404-ing later.
        """
        assert list(_downloader(tmp_path).iter_quarters(2099, 1)) == []

    def test_unbounded_run_stops_before_the_current_quarter(self, tmp_path):
        """Without an end bound the range still terminates."""
        got = list(_downloader(tmp_path).iter_quarters(2024, 1))
        assert got, "Should yield at least the 2024 quarters"
        assert got == sorted(got)
        assert len(got) < 100, "Unbounded iteration must terminate"


# ======================================================================
# Rate limiting
# ======================================================================

class TestRateLimit:

    def test_default_interval_respects_sec_fair_access(self, tmp_path):
        """
        The SEC caps programmatic access at 10 req/s. config sets 8.0, so the
        minimum spacing between requests must be at least 1/10s.
        """
        dl = EdgarDownloader(tmp_path, USER_AGENT, rate_limit_rps=config.edgar_rate_limit_rps)
        assert config.edgar_rate_limit_rps <= 10.0
        assert dl._min_interval >= 0.1
