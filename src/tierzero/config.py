from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

_PROJECT_ROOT = Path(__file__).parent.parent.parent


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
        default_factory=lambda: os.getenv("SEC_USER_AGENT", "tierzero research@example.com")
    )
    edgar_dataset_url_pattern: str = (
        "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/"
    )
    edgar_bulk_url_pattern: str = (
        "https://www.sec.gov/dera/data/financial-statements/{year}q{quarter}.zip"
    )
    edgar_tickers_url: str = "https://www.sec.gov/files/company_tickers.json"

    # SEC fair-access policy: stay well under 10 req/s
    edgar_rate_limit_rps: float = 8.0

    # --- yfinance ---
    yfinance_batch_size: int = 50
    prices_start_date: str = "1993-01-01"

    # --- Historical range for full bootstrap ---
    start_year: int = 2000
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


config = Config()
