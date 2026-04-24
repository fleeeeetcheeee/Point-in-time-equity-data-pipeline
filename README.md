# Point-in-Time Equity Data Pipeline

A reproducible pipeline that assembles survivorship-bias-free US equity data with point-in-time fundamentals. Given a `(ticker, date)` pair, returns every piece of data — price, earnings, balance sheet items, index membership, market cap — **as it would have been known at market close on that date**.

## Why this matters

Most backtest data pipelines have one or both of these silent correctness failures:

**1. Lookahead via wrong date anchor**
Apple's fiscal Q4 2023 period ended December 28, 2023 — but the 10-Q wasn't *filed* until February 2, 2024. A pipeline that uses `period_of_report` as the availability date will assume Q4 earnings were known on January 1, injecting 32 days of lookahead into every backtest that touches Apple. This pipeline uses `filed_date` (the SEC acceptance timestamp) as the sole availability gate.

**2. Survivorship bias**
Backtesting "the S&P 500" using the *current* constituent list implicitly excludes every company that was ever removed — Lehman Brothers, Enron, Bear Stearns, and thousands of others. This pipeline tracks historical index membership and includes delisted companies.

## Architecture

```
data/
├── raw/                        # immutable downloads
│   ├── edgar/{year}q{quarter}/ # sub.txt, num.txt, tag.txt
│   ├── prices/                 # per-ticker OHLCV Parquet
│   └── sp500_membership/       # Wikipedia scrape CSV
└── processed/
    ├── prices/date=YYYY-MM-DD/ # Hive-partitioned daily prices
    ├── fundamentals/as_filed/  # one Parquet per EDGAR quarter
    └── index_membership/       # sp500_timeline.parquet

src/tierzero/
├── config.py                   # paths, rate limits, env vars
├── ingestion/                  # download-only, no transformation
│   ├── edgar.py                # SEC EDGAR quarterly XBRL zips
│   ├── prices.py               # yfinance OHLCV + corporate actions
│   └── sp500_membership.py     # Wikipedia constituent changes
├── processing/                 # transform raw → processed
│   ├── edgar_parser.py         # XBRL → Parquet (filed_date PIT anchor)
│   ├── price_cleaner.py        # clean OHLCV, PIT-correct split adjustment
│   └── membership_builder.py   # event stream → (ticker, entry, exit) timeline
├── storage/
│   ├── writer.py               # atomic Parquet writes, Hive partitioning
│   └── reader.py               # DuckDB views over Parquet store
└── pit/
    └── lookup.py               # EquitySnapshot + PointInTimeLookup (core API)
```

## Data sources (all free)

| Data | Source |
|------|--------|
| Daily OHLCV + corporate actions | [yfinance](https://github.com/ranaroussi/yfinance) |
| Point-in-time fundamentals | [SEC EDGAR Financial Statement Data Sets](https://www.sec.gov/dera/data/financial-statements) |
| S&P 500 membership history | [Wikipedia](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies) + [fja05680/sp500](https://github.com/fja05680/sp500) |
| CIK ↔ ticker mapping | [SEC company_tickers.json](https://www.sec.gov/files/company_tickers.json) |

## Setup

```bash
git clone https://github.com/fleeeeetcheeee/Point-in-time-equity-data-pipeline.git
cd Point-in-time-equity-data-pipeline

python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

cp .env.example .env
# Edit .env: set SEC_USER_AGENT="Your Name your@email.com"
```

## Run the bootstrap (one-time)

```bash
# Full history from 2000 (4–8 hours, ~50 GB raw)
python scripts/bootstrap.py

# Lighter run for testing (single year)
python scripts/bootstrap.py --start-year 2020 --start-quarter 1
```

## Query the data

```python
from datetime import date
from pathlib import Path

import pandas as pd

from tierzero.config import config
from tierzero.processing.membership_builder import MembershipBuilder
from tierzero.storage.reader import DataReader
from tierzero.pit.lookup import PointInTimeLookup

reader = DataReader(config.data_root)
membership = MembershipBuilder.load(config.processed_membership_path)
ticker_cik = pd.read_parquet(config.data_root / "processed" / "cik_ticker_map.parquet")

pit = PointInTimeLookup(reader, membership, ticker_cik)

# Single ticker lookup
snap = pit.get("AAPL", date(2024, 1, 15))
print(snap.close)               # unadjusted close on 2024-01-15
print(snap.revenue_ttm)         # TTM revenue as known on 2024-01-15 (Q3 FY2023 data)
print(snap.latest_filing_date)  # 2023-11-03 — the Q3 10-Q filing date
print(snap.in_sp500)            # True

# Full S&P 500 universe on a date (for backtesting)
universe = pit.get_universe(date(2024, 1, 15), universe="sp500")
```

## Run the tests

```bash
pytest                          # all tests with coverage
pytest tests/unit/              # unit tests only (no data required)
pytest tests/integration/       # PIT leakage tests
```

The integration tests in `tests/integration/test_no_lookahead.py` are the **done criterion** for the pipeline. They verify using synthetic data that:
- Fundamentals filed after the query date are invisible
- The exact filing-date boundary is respected (`<=` not `<`)
- `period_of_report` is never used as an availability gate
- Delisted companies retain their historical data
- Amended filings (10-K/A) supersede originals only from their own `filed_date` onward
- TTM figures use only filings known by the query date

## Daily update

```bash
python scripts/daily_update.py  # run after market close
```

## Key design decisions

**`filed_date` not `period_of_report`** — The SEC acceptance timestamp is when data becomes publicly observable. Period end dates can precede filings by 30–75 days depending on company size and form type.

**`auto_adjust=False` for prices** — yfinance's auto-adjust retroactively modifies all historical prices when new splits occur. Storing unadjusted prices with a separate corporate-actions log lets us compute the correct split-adjusted price for any `(ticker, as_of_date)` without corrupting earlier queries.

**Polars for EDGAR parsing, DuckDB for queries** — A single `num.txt` can be 2–5 GB. Polars handles this without OOM. DuckDB provides SQL with predicate pushdown over Hive-partitioned Parquet, so date-range queries scan only the relevant partition directories.

**Atomic writes** — All Parquet files are written to a `.tmp` path then renamed. A partial write is never visible to readers.
