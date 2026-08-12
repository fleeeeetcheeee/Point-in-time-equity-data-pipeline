# Point-in-Time Equity Data Pipeline

A reproducible pipeline that assembles survivorship-bias-free US equity data with point-in-time fundamentals. Given a `(ticker, date)` pair, returns every piece of data — price, earnings, balance sheet items, index membership, market cap — **as it would have been known at market close on that date**.

## Status

Runs end-to-end against live SEC and Yahoo data. 113 tests pass, 90% coverage;
the 18 leakage tests in `tests/integration/test_no_lookahead.py` are the
acceptance criterion and all pass. A universe query for a single date over a
30-ticker, 33-year store returns in ~1.4s.

Verified on a smoke run (one EDGAR quarter, 40 tickers), **not** a full
historical bootstrap. Price coverage for delisted companies is materially
incomplete — see [Limitations](#limitations) before using this for research.

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
| Point-in-time fundamentals | [SEC EDGAR Financial Statement Data Sets](https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets) |
| S&P 500 membership history | [Wikipedia: Historical components of the S&P 500](https://en.wikipedia.org/wiki/Historical_components_of_the_S%26P_500) + [fja05680/sp500](https://github.com/fja05680/sp500) |
| CIK ↔ ticker mapping | [SEC company_tickers.json](https://www.sec.gov/files/company_tickers.json) |

## Setup

```bash
git clone https://github.com/fleeeeetcheeee/Point-in-time-equity-data-pipeline.git
cd Point-in-time-equity-data-pipeline

python -m venv .venv && source .venv/bin/activate

# Reproducible: the exact versions this has been verified against
pip install -r requirements.lock && pip install -e . --no-deps

# Or, for development against current versions
pip install -e ".[dev]"

cp .env.example .env
# Edit .env: set SEC_USER_AGENT="Your Name your@email.com"
```

`SEC_USER_AGENT` is required, not optional. The SEC blocks programmatic access
without a real contact address and bans IPs that ignore this, so both scripts
refuse to start until it is set.

## Run the bootstrap (one-time)

```bash
# Smoke run: one EDGAR quarter, 40 tickers — a few minutes, ~1 GB
python scripts/bootstrap.py \
    --start-year 2026 --start-quarter 1 \
    --end-year 2026 --end-quarter 1 --max-tickers 40

# Full history from 2009 (4–8 hours, ~50 GB raw)
python scripts/bootstrap.py
```

The default start is 2009q1 because that is where the SEC's Financial Statement
Data Sets begin; earlier quarters do not exist. Price history reaches back to
1993 independently of this.

Bound the range before trusting a "quick" run: `--start-year 2024` alone is ten
quarters and roughly 25 GB of EDGAR data. `--end-year`/`--end-quarter` cap the
EDGAR range and `--max-tickers` caps the price download, which is what makes a
smoke run genuinely cheap.

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

## Limitations

Read this section before building anything on top of this pipeline. Several of
these are material to backtest validity.

**`close_adj` in the store is not point-in-time correct.** It is computed at
write time using every split known then, so it embeds future information by
construction. It exists as a convenience for charting and sanity checks. Any
research use must take `close_unadj` and apply
`PriceCleaner.get_adjusted_close()` with factors restricted to the query date —
which is what `PointInTimeLookup` does for the `close` field. The two columns
sitting side by side in the same table is the sharpest edge in this codebase.

**Price history is survivorship-biased even though membership is not.** This is
the most important caveat here, because it partially defeats the project's
stated purpose. The membership timeline correctly includes every company that
was ever in the index, but yfinance does not serve price history for many
delisted or acquired tickers. In a 40-ticker smoke run, 10 returned nothing at
all — Ambac, Abiomed, AK Steel, Alexion, Alpha Natural Resources, Airgas,
Arconic, Activision Blizzard and others. A backtest run on this store will
therefore silently omit roughly a quarter of the historical universe, skewed
towards exactly the failures and takeovers that matter most. Fixing this
properly needs a paid survivorship-bias-free source (CRSP, Norgate, Sharadar);
without one, treat long-horizon results from this data as optimistic.

**Fundamentals start in 2009, prices in 1993.** The SEC's Financial Statement
Data Sets do not exist before 2009q1. Any strategy needing fundamentals cannot
be tested on the 2000–2008 period from this source, and the two datasets have
different coverage windows — a universe screen that assumes both are present
will quietly shrink before 2009.

**Index membership comes from Wikipedia, not an authoritative source.** The
scrape yields 407 change events back to 1976, but Wikipedia is neither complete
nor guaranteed accurate, particularly for older entries, and it has already
moved between articles once during this project. CRSP is the authoritative
source. The spec's suggestion of reconstructing membership from SEC 13F filings
remains unimplemented.

**Universe is the S&P 500, not the Russell 3000** named in the project spec.
This is a deliberate scope reduction — S&P 500 membership history is freely
available and Russell's is not — but it means the universe is large-cap only,
and small-cap effects cannot be studied with it.

**Verified on a smoke run, not a full one.** The pipeline has been run
end-to-end against live SEC and Yahoo data for one EDGAR quarter and 40
tickers. The full 2009–present, ~1,100-ticker bootstrap has not been executed,
so quarter-to-quarter schema drift in older EDGAR datasets is unmeasured.

**`get_universe` is fast, `get` in a loop is not.** Universe queries are
batched into a single SQL statement; calling `get()` per ticker instead
reintroduces an N+1 that measured ~90× slower. Backtests should call
`get_universe` once per rebalance date.

**No intraday, no fundamentals restatement history.** Daily bars only. The
store keeps each filing as filed, so restatements are visible as later
filings — but there is no reconciliation of how a given fact changed over time.
