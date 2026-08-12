from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

_PROJECT_ROOT = Path(__file__).parent.parent.parent

# What edgar_user_agent falls back to when SEC_USER_AGENT is unset. It has the
# shape the SEC asks for but the address is fake, which is the point of the
# header: the SEC contacts you before banning the IP. Running against the live
# API with this is a fair-access violation, so scripts refuse to start on it.
PLACEHOLDER_USER_AGENT = "tierzero research@example.com"


@dataclass
class Config:
    project_root: Path = field(default_factory=lambda: _PROJECT_ROOT)
    data_root: Path = field(default_factory=lambda: Path(os.getenv("DATA_ROOT", str(_PROJECT_ROOT / "data"))))

    # --- Storage paths (derived in __post_init__) ---
    raw_edgar_dir: Path = field(default=None)
    raw_prices_dir: Path = field(default=None)
    raw_membership_dir: Path = field(default=None)
    processed_prices_dir: Path = field(default=None)
    processed_fundamentals_dir: Path = field(default=None)
    processed_membership_path: Path = field(default=None)

    # --- SEC EDGAR ---
    # The SEC requires a descriptive User-Agent for all programmatic access.
    # Without it requests are blocked; repeat violations get the IP banned.
    # Format: "CompanyName contact@email.com"
    edgar_user_agent: str = field(
        default_factory=lambda: os.getenv("SEC_USER_AGENT", PLACEHOLDER_USER_AGENT)
    )
    edgar_dataset_url_pattern: str = (
        "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/"
    )
    # The SEC has moved this dataset at least twice. The old
    # /dera/data/financial-statements/ path 404s as of 2026-08; the canonical
    # location is now under /files/. test_ingestion_edgar.py pins the shape of
    # this URL so a future move fails a test instead of a bootstrap run.
    edgar_bulk_url_pattern: str = (
        "https://www.sec.gov/files/dera/data/financial-statement-data-sets/"
        "{year}q{quarter}.zip"
    )
    edgar_tickers_url: str = "https://www.sec.gov/files/company_tickers.json"

    # SEC fair-access policy: stay well under 10 req/s
    edgar_rate_limit_rps: float = 8.0

    # --- yfinance ---
    yfinance_batch_size: int = 50
    prices_start_date: str = "1993-01-01"

    # --- Historical range for full bootstrap ---
    # The SEC's Financial Statement Data Sets begin at 2009q1; earlier quarters
    # 404 (verified 2026-08-12). Starting at 2000 bought 36 quarters of
    # guaranteed failures, not 9 extra years of fundamentals. Prices reach
    # further back — see prices_start_date — so the two coverage windows differ.
    start_year: int = 2009
    start_quarter: int = 1

    def __post_init__(self) -> None:
        self.raw_edgar_dir = self.data_root / "raw" / "edgar"
        self.raw_prices_dir = self.data_root / "raw" / "prices"
        self.raw_membership_dir = self.data_root / "raw" / "sp500_membership"
        self.processed_prices_dir = self.data_root / "processed" / "prices"
        self.processed_fundamentals_dir = (
            self.data_root / "processed" / "fundamentals" / "as_filed"
        )
        self.processed_membership_path = (
            self.data_root / "processed" / "index_membership" / "sp500_timeline.parquet"
        )

    def require_real_user_agent(self) -> None:
        """
        Refuse to run against the live SEC API with the placeholder contact.

        Raises rather than warns: a warning scrolls past in a job that runs for
        hours, and the consequence of ignoring it is an IP ban.
        """
        if self.edgar_user_agent == PLACEHOLDER_USER_AGENT:
            raise SystemExit(
                "SEC_USER_AGENT is not set, so requests would identify themselves with a "
                f"placeholder address ({PLACEHOLDER_USER_AGENT!r}).\n"
                "The SEC requires a real contact and bans IPs that ignore this.\n"
                'Set it in .env:  SEC_USER_AGENT="Your Name your@email.com"'
            )


config = Config()
