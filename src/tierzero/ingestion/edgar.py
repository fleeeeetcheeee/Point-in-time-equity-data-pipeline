"""
SEC EDGAR Financial Statement Data Set downloader.

Each quarterly zip contains three TSV files:
  sub.txt  — one row per filing submission (adsh, cik, name, form, filed, period, ...)
  num.txt  — one row per numeric XBRL fact   (adsh, tag, ddate, qtrs, uom, value, ...)
  tag.txt  — XBRL taxonomy tag definitions

The 'filed' column in sub.txt is the point-in-time anchor for all fundamental
queries. The 'period' / 'ddate' columns represent the period of report and must
never be used to determine when data became publicly known.
"""

from __future__ import annotations

import logging
import time
import zipfile
from datetime import date
from pathlib import Path
from typing import Iterator

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

log = logging.getLogger(__name__)

_REQUIRED_FILES = {"sub.txt", "num.txt", "tag.txt"}


class EdgarDownloader:
    """
    Downloads SEC EDGAR quarterly Financial Statement Data Set zips.

    The bulk structured datasets live at:
      https://www.sec.gov/dera/data/financial-statements/{year}q{quarter}.zip

    Usage::

        downloader = EdgarDownloader(raw_edgar_dir, user_agent="Name email@x.com")
        for year, quarter in downloader.iter_quarters(2010, 1):
            downloader.download_quarter(year, quarter)
    """

    BULK_URL = "https://www.sec.gov/dera/data/financial-statements/{year}q{quarter}.zip"

    def __init__(
        self,
        output_dir: Path,
        user_agent: str,
        rate_limit_rps: float = 8.0,
    ) -> None:
        self.output_dir = output_dir
        self._min_interval = 1.0 / rate_limit_rps
        self._last_request: float = 0.0

        self._session = requests.Session()
        # SEC fair-access policy requires a descriptive User-Agent.
        # Requests without one are blocked; repeated violations trigger IP bans.
        self._session.headers.update({"User-Agent": user_agent})

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def download_quarter(self, year: int, quarter: int) -> Path:
        """
        Download and extract the XBRL bulk zip for one quarter.

        Returns the directory containing sub.txt / num.txt / tag.txt.
        Skips the download if the directory already exists and is complete.
        """
        dest_dir = self.output_dir / f"{year}q{quarter}"

        if self._is_complete(dest_dir):
            log.info("EDGAR %dq%d already downloaded, skipping.", year, quarter)
            return dest_dir

        url = self.BULK_URL.format(year=year, quarter=quarter)
        log.info("Downloading EDGAR %dq%d from %s", year, quarter, url)

        dest_dir.mkdir(parents=True, exist_ok=True)
        zip_path = dest_dir / "_download.zip"

        self._download_file(url, zip_path)
        self._extract(zip_path, dest_dir)
        zip_path.unlink(missing_ok=True)

        log.info("EDGAR %dq%d extracted to %s", year, quarter, dest_dir)
        return dest_dir

    def iter_quarters(
        self, start_year: int, start_quarter: int
    ) -> Iterator[tuple[int, int]]:
        """
        Yield (year, quarter) pairs from (start_year, start_quarter) up to
        the most recently completed quarter (roughly current date minus 45 days).
        """
        today = date.today()
        y, q = start_year, start_quarter

        while True:
            # Quarter ends in month q*3; add 45-day lag for SEC processing
            quarter_end_month = q * 3
            try:
                quarter_end = date(y, quarter_end_month, 1)
            except ValueError:
                break

            days_since_end = (today - quarter_end).days
            if days_since_end < 45:
                break

            yield y, q

            q += 1
            if q > 4:
                q = 1
                y += 1

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request = time.monotonic()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        reraise=True,
    )
    def _download_file(self, url: str, dest: Path) -> None:
        self._throttle()
        with self._session.get(url, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            with open(dest, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    fh.write(chunk)

    @staticmethod
    def _extract(zip_path: Path, dest_dir: Path) -> None:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(dest_dir)

    @staticmethod
    def _is_complete(dest_dir: Path) -> bool:
        if not dest_dir.exists():
            return False
        present = {f.name for f in dest_dir.iterdir()}
        return _REQUIRED_FILES.issubset(present)
