# What We Built and How It Works
### A Beginner-Friendly Explanation

---

## The Big Picture

Imagine you want to test a trading strategy — say, "buy stocks with low P/E ratios every January." To test whether this strategy would have made money historically, you need to simulate going back in time and making decisions using only the information that was available *at that moment*.

The problem is: getting historical data right is surprisingly hard. Most beginners (and even some professionals) make subtle mistakes that cause their backtests to look far better than they really are. This pipeline is specifically designed to avoid those mistakes.

---

## The Two Big Problems We Solved

### Problem 1: Lookahead Bias (Using Future Information)

Here's a concrete example. Apple announces its quarterly earnings on **February 2, 2024** — that's the day the report is filed with the SEC. The earnings cover the period ending **December 28, 2023**.

If you're simulating a trade on **January 15, 2024**, you should NOT know Apple's December earnings yet. They weren't announced until February.

But many data pipelines make this mistake. They look at "what quarter did this earnings cover?" (December 2023) and assume the data was available in December 2023. That's wrong — it wasn't public until February 2, 2024.

**What we do instead:** Every earnings number is tagged with its `filed_date` (when the SEC actually received and published it). When you ask "what did Apple's earnings look like on January 15?", we only show you filings where `filed_date <= January 15`. The Q4 earnings don't appear until after February 2.

This is called **point-in-time correctness** — the whole pipeline is named after it.

### Problem 2: Survivorship Bias (Forgetting Dead Companies)

If you look up "S&P 500 companies" today, you'll get a list of 500 healthy, successful companies. But if you backtest using *only* those companies going back to 2000, you're cheating — because you're only including companies that *survived* to today.

All the companies that went bankrupt or were removed from the index (Lehman Brothers, Enron, Bear Stearns, hundreds of others) are missing from your backtest. Of course your strategy looks great — you filtered out all the disasters in advance.

**What we do instead:** We track the *historical* list of S&P 500 members — who was added, who was removed, and when. When you ask "what companies were in the S&P 500 on September 1, 2008?", you get the real list including Lehman Brothers, which collapsed just two weeks later.

---

## Where the Data Comes From

We use three free data sources:

| Data | Source | Why |
|------|--------|-----|
| Stock prices | yfinance (Yahoo Finance) | Free, reliable daily prices going back decades |
| Earnings & financials | SEC EDGAR | The US government's official database of all public company filings. Free and comprehensive. |
| S&P 500 history | Wikipedia | Has a table of every company ever added/removed from the index |

---

## How the Code is Organized

Think of the pipeline as an assembly line with four stages:

```
[Download] → [Process] → [Store] → [Query]
```

### Stage 1: Download (`src/tierzero/ingestion/`)

These files are responsible for fetching raw data from the internet and saving it locally. Nothing is transformed here — we save everything exactly as received.

- **`edgar.py`** — Downloads quarterly zip files from the SEC. Each zip contains three spreadsheets listing every financial filing made that quarter by every public company in the US.
- **`prices.py`** — Downloads daily stock prices via yfinance. Importantly, we download *unadjusted* prices (more on why below).
- **`sp500_membership.py`** — Scrapes the Wikipedia page that lists every time a company was added to or removed from the S&P 500.

### Stage 2: Process (`src/tierzero/processing/`)

These files transform the raw data into clean, structured formats.

- **`edgar_parser.py`** — Reads the SEC spreadsheets and extracts the financial numbers we care about (revenue, profit, assets, etc.). Crucially, it records the `filed_date` for each number, not the period it covers. This is where we enforce the no-lookahead rule.
- **`price_cleaner.py`** — Cleans up messy price data (removes bad entries, flags suspicious gaps). Also handles **split adjustments** — when a company does a stock split, all historical prices need to be rescaled. We do this in a point-in-time-correct way: a query from before the split doesn't use any split adjustments that hadn't happened yet.
- **`membership_builder.py`** — Takes the Wikipedia add/remove events and builds a clean timeline: "TESTCO was in the S&P 500 from Jan 2005 to Jan 2008, then re-added in Jun 2010."

### Stage 3: Store (`src/tierzero/storage/`)

Processed data is saved as **Parquet files** — a compressed, efficient file format for tabular data (think of it as a much faster, smaller version of CSV).

- **`writer.py`** — Saves processed data. Price files are organized by date in folders (one folder per day). Financial data is organized by quarter. All saves are *atomic* — meaning a file is either fully written or not written at all, never half-written.
- **`reader.py`** — Loads data using **DuckDB**, which lets us run SQL queries directly against the Parquet files without loading everything into memory. This is important when you have years of data.

### Stage 4: Query (`src/tierzero/pit/lookup.py`)

This is the final product — the API you actually use.

**`PointInTimeLookup`** is the main class. You give it a ticker and a date, and it returns an **`EquitySnapshot`** — a complete picture of that company as of that date.

```python
snap = pit.get("AAPL", date(2024, 1, 15))
snap.close          # closing price on Jan 15, 2024
snap.revenue_ttm    # trailing 12-month revenue KNOWN by Jan 15
                    # (Q3 2023 data — Q4 wasn't filed yet)
snap.in_sp500       # was Apple in the S&P 500 on that date?
snap.pe_ratio       # price-to-earnings ratio as of that date
```

The `EquitySnapshot` also tells you *which filing* its numbers came from (`latest_filing_date`, `latest_filing_adsh`) so you can verify exactly what data was used.

---

## The Stock Split Problem (Explained Simply)

When Apple did a 4-for-1 split in August 2020, every share became 4 shares worth ¼ the price. A stock that was $400 before the split became $100 after.

To compare prices before and after the split, you need to divide all pre-split prices by 4. This is called "split adjustment."

The problem: if you're simulating a trade on January 1, 2020 (before the split), you didn't *know* about the split yet. Using split-adjusted prices in a January 2020 backtest is another form of using future information.

Our solution: we store the raw unadjusted prices, and when we need an adjusted price for a given date, we only apply splits that were known *by that date*.

---

## The Scripts

- **`scripts/bootstrap.py`** — Run this once to download all historical data. It takes 4–8 hours and about 50 GB of disk space. You can test it with a smaller range first (`--start-year 2020`).
- **`scripts/daily_update.py`** — Run this after market close each day to keep prices and filings current.

---

## The Tests

The tests in `tests/` verify that the pipeline actually does what it claims. The most important ones are in `tests/integration/test_no_lookahead.py`.

These tests work by creating fake data with known values, then asking: "if we query on date X, do we get the right data — and only the data that was known by date X?"

For example, one test creates two fake earnings filings — one filed in November 2023 and one filed in February 2024 — then checks:
- A December 2023 query returns only the November filing ✓
- A March 2024 query returns the February filing ✓
- A January 2024 query (after the period ended but before filing) returns only the November filing ✓ ← this is the critical one

The last check is what separates this pipeline from naive implementations that use `period_of_report` instead of `filed_date`.

---

## Quick Reference: Key Terms

| Term | Meaning |
|------|---------|
| `filed_date` | The date the SEC received and published a filing. **This is when the data became public.** |
| `period_of_report` | The last day of the quarter the filing covers. Not the same as when it became public. |
| Point-in-time (PIT) | Data as it was known at a specific historical moment, with no future information. |
| Survivorship bias | The distortion caused by only analyzing companies that survived, excluding failures. |
| TTM (Trailing Twelve Months) | The sum of the last four quarters, used for annual comparisons of quarterly filers. |
| XBRL | The structured data format the SEC uses for financial filings. Lets us extract specific numbers (revenue, assets, etc.) without reading PDFs. |
| Parquet | A compressed file format for tabular data. Much faster to query than CSV. |
| DuckDB | An in-process SQL engine that can query Parquet files directly. Think SQLite but for analytics. |
| Hive partitioning | Organizing Parquet files into folders by date (`date=2024-01-15/`) so queries only read the files they need. |
| Accession number (`adsh`) | The SEC's unique ID for each filing. Looks like `0000320193-24-000010`. |
| CIK | The SEC's unique ID for each company. Apple is `320193`. |
