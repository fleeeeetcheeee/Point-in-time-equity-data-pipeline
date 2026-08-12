# Project 1 — Point-in-Time Equity Data Pipeline

**Tier:** 0 (Infrastructure)
**Spec:** `ResearchToDo.md` → Part 3 → Tier 0 → Project 1
**Repo:** https://github.com/fleeeeetcheeee/Point-in-time-equity-data-pipeline
**Status:** Done criterion **met and now verified against live data**. Suite green — 113 passed,
90% coverage. Bootstrap runs end-to-end (smoke scale: one EDGAR quarter, 40 tickers); the full
historical run has not been executed. See [Open items](#open-items).

> **Note on provenance.** Entries dated 2026-04-24 were reconstructed on 2026-08-07 from the
> repository's git history plus `README.md` and `EXPLANATION.md`. They record what was built and
> the reasoning that survived into the docs; they are not contemporaneous notes, so any dead ends
> or discarded approaches from that session are not captured. Entries from 2026-08-07 onward are
> written as work happens.

---

## Goal

Given a `(ticker, date)` pair, return every piece of data — price, earnings, balance sheet items,
index membership, market cap — **as it would have been known at market close on that date**.

**Done criterion (from the spec):** the above, with unit tests proving it.

---

## Build log

### 2026-04-24 — Full initial build (9 commits, ~10:06–11:31)

The entire pipeline was built in one sitting, committed layer by layer in dependency order.

**`eca9ba2` · 10:06 · Initial commit**
Bare README stub.

**`3927296` · 10:59 · Project scaffold**
`pyproject.toml` (hatchling, `src/` layout, Python ≥3.11), `config.py`, package skeleton, data
directory layout, `.gitignore`, `.env.example`. 14 files, 165 lines.

Decisions locked in here:
- Single `Config` dataclass owns every path, URL, and rate limit; derived paths computed in
  `__post_init__`. No data path is hardcoded anywhere else in the codebase.
- `data/raw/` and `data/processed/` are gitignored — the repo ships the pipeline, not the data.
  Directory shape is preserved with `.gitkeep` files.
- `SEC_USER_AGENT` is required via `.env`. The SEC blocks programmatic access without a
  descriptive User-Agent and bans the IP on repeat violations. Rate limit set to 8.0 req/s against
  the SEC's 10 req/s fair-access ceiling.

**`0dff31f` · 11:10 · Ingestion layer** — 3 files, 474 lines.
`edgar.py` (quarterly XBRL zips), `prices.py` (yfinance OHLCV + corporate actions),
`sp500_membership.py` (Wikipedia constituent-change scrape).

Architectural rule established: ingestion **downloads and writes bytes verbatim**. No parsing, no
cleaning, no transformation. Anything that interprets the bytes belongs in `processing/`.

**`856ca86` · 11:22 · Processing layer** — 3 files, 627 lines.
This is where point-in-time correctness is actually enforced:
- `edgar_parser.py` — XBRL → Parquet, anchoring every fact to `filed_date`.
- `price_cleaner.py` — OHLCV cleaning, outlier flagging, PIT-correct split adjustment.
- `membership_builder.py` — add/remove event stream → `(ticker, entry, exit)` interval timeline,
  handling re-entry (a company can leave and rejoin the index).

**`d8bbfda` · 11:25 · Storage layer** — 2 files, 244 lines.
`writer.py`: atomic writes (write to `.tmp`, then rename) so a partial write is never visible to a
reader; Hive partitioning (`date=YYYY-MM-DD/`). `reader.py`: DuckDB SQL views over the Parquet
store, so date-range queries get predicate pushdown and scan only relevant partition directories.

**`6666aae` · 11:26 · PIT lookup API** — 440 lines, the largest single module.
`EquitySnapshot` dataclass + `PointInTimeLookup`, including TTM construction from the last four
quarterly filings *known by the query date*. `EquitySnapshot` carries `latest_filing_date` and
`latest_filing_adsh` so any returned number can be traced back to the exact filing it came from.
`get_universe(date, universe="sp500")` returns the historically correct constituent list.

**`d8fd686` · 11:29 · Test suite** — 5 files, 857 lines.
Unit tests for parser/cleaner/membership plus `tests/integration/test_no_lookahead.py` (332 lines),
which is the project's done criterion. Uses synthetic data with known values to assert:
- Fundamentals filed after the query date are invisible.
- The filing-date boundary is inclusive (`<=`, not `<`).
- `period_of_report` is never used as an availability gate.
- Delisted companies retain their historical data.
- Amended filings (10-K/A) supersede originals only from the amendment's own `filed_date` forward.
- TTM figures use only filings known by the query date.

The critical case: two synthetic filings, one filed 2023-11, one filed 2024-02. A January 2024
query — after the period ended but before the filing — must return only the November filing. This
is exactly what a `period_of_report`-gated pipeline gets wrong.

**`47abadd` · 11:30 · Scripts** — 2 files, 370 lines.
`bootstrap.py` (full history from 2000; ~4–8 h, ~50 GB raw; `--start-year`/`--start-quarter` for
cheap smoke runs) and `daily_update.py` (incremental, run after market close).

**`3c5d71d` · 11:31 · README**
Architecture, setup, usage examples, design decisions. +137 lines.

---

### Undated, uncommitted (present in working tree as of 2026-08-07)

- **`EXPLANATION.md`** (untracked) — plain-language companion to the README explaining lookahead
  bias, survivorship bias, the split-adjustment problem, and the four-stage architecture to a
  non-expert reader. Includes a key-terms glossary.
- **`scripts/.gitkeep`** deleted (superseded by the two real scripts) — deletion not yet staged.

---

### 2026-08-07 — Restructure + log reconstruction

Moved the repository from `Portfolio/TierZero/` to
`Portfolio/TierZero/Project01-PointInTimeEquityDataPipeline/` so it matches the portfolio
convention of *tier folder → project folder → project repo*. Git history, remote, and working-tree
state verified intact after the move. Created this log. Nothing committed.

### 2026-08-07 — First test run (open item 1 closed)

Created `.venv` (Python 3.13.9, gitignored) and ran `pip install -e ".[dev]"`. Clean install, no
build failures. First execution of the suite in this environment.

**Result: 44 passed, 1 failed, 1.19s. Coverage 30% (639 statements, 448 uncovered).**

**The done criterion passes.** All 18 tests in `tests/integration/test_no_lookahead.py` pass. The
`filed_date` gating, inclusive boundary, delisting retention, amendment supersession, and
TTM-known-by-date behavior are all verified against synthetic fixtures.

**Failure: `tests/unit/test_price_cleaner.py::TestClean::test_non_positive_close_removed`** —
`KeyError: 'close_unadj'`.

This is a **bad assertion in the test, not a defect in the pipeline.** The two methods have
different output schemas by design:
- `PriceCleaner.clean()` drops/flags rows and returns yfinance's capitalized columns (`Close`).
- `PriceCleaner.to_processed_df()` calls `clean()`, then renames to the storage schema
  (`close_unadj`, `open`, `high`, `low`, `volume`) and derives `close_adj` (price_cleaner.py:170).

The test calls `clean()` but asserts on `cleaned["close_unadj"]`, a name that only exists after
`to_processed_df()`. The two sibling tests in the same class call `clean()` and assert on
`volume_zero`/`ticker`, which `clean()` does produce — so only this one assertion is wrong.

Production path confirmed unaffected: `bootstrap.py:148` and `daily_update.py:91` both call
`to_processed_df()`, so the columns the writer schema (`writer.py:43`) and `lookup.py:205` expect
are the ones actually produced.

Fix is one line — assert on `"Close"`, or point the test at `to_processed_df()`. Not applied yet.

**Coverage gaps worth noting:** `ingestion/` 0%, `storage/writer.py` 0%, `config.py` 0%. Nothing
that performs network I/O or writes Parquet has ever executed. `pit/lookup.py` is at 40% —
the integration tests exercise the PIT gating paths but not most of the query surface.

### 2026-08-07 — Assertion fixed, round-trip test added (suite green)

**1. Fixed `test_non_positive_close_removed`.** Now asserts on `Close` (what `clean()` actually
emits) and additionally that the bad row is dropped (`len == 2`), which is the behavior the test
name promises and was never actually checked. Comment added explaining that the rename to the
storage schema belongs to `to_processed_df()`. No source change — the bug was entirely in the test.

**2. Added `tests/integration/test_storage_roundtrip.py`** (11 tests) covering the seam no existing
test reached: raw yfinance frame → `to_processed_df` → `ParquetWriter` → DuckDB `DataReader` →
`PointInTimeLookup.get()`. Real Parquet files on a `tmp_path` root, real DuckDB, no mocks and no
hand-built storage-schema fixtures — the only input is a raw capitalized-column OHLCV frame.

The reshape between the cleaner and the writer (`bootstrap.py:153-159`) is duplicated inside the
test on purpose: that duplication is what makes a divergence between the scripts' column list and
the cleaner's output fail here instead of four hours into a bootstrap run.

Covers: schema conformance against `PRICE_SCHEMA`, bad rows dropped before storage, Hive partition
layout, no `.tmp.parquet` left behind after atomic write, value fidelity through the round trip,
every column `lookup.py`'s SQL selects being present in the view, weekend fallback to the prior
trading day through real SQL, no-price-before-data-exists, membership resolution, unknown ticker.

**3. Verified the new test can actually fail.** A passing test proves nothing until you have seen
it fail for the right reason. Mutated `price_cleaner.py:170` to rename `Close` → `close_unadjusted`
instead of `close_unadj` — a realistic drift, and one that would break every bootstrap run:

| Suite | Against mutated source |
|---|---|
| `tests/unit` + `test_no_lookahead.py` (the pre-existing suite) | **45 passed** — gap confirmed real |
| `test_storage_roundtrip.py` (new) | **1 failed, 10 errors** — drift caught |

Source restored and verified identical to HEAD (`git diff` on `src/` is empty). The pre-existing
suite passing 45/45 against that mutation is the concrete demonstration that this gap was worth
closing — it is not a hypothetical.

**Result: 56 passed, 0 failed. Coverage 30% → 52%.**

| Module | Before | After |
|---|---|---|
| `pit/lookup.py` | 40% | 92% |
| `storage/writer.py` | 0% | 87% |
| `storage/reader.py` | 38% | 74% |
| `processing/price_cleaner.py` | 78% | 92% |
| `processing/membership_builder.py` | 71% | 83% |
| `ingestion/*`, `config.py` | 0% | 0% (unchanged — network I/O) |

**Dependency drift:** `pyproject.toml` uses open lower bounds, so this install resolved
pandas 3.0.5, numpy 2.5.1, polars 1.43.2, duckdb 1.5.5, pytest 9.1.1 — substantially newer than
what the April build was written against. Everything still passes, but the Part 4 standard
("code that runs, with pinned dependencies") argues for a lockfile before this is presented.

### 2026-08-12 — First live run. Five real bugs, none of which the suite could see.

The 56-test suite was green and the pipeline had never touched a real server. Running it found
that **the fundamentals half of the pipeline had been broken for the entire life of the project**,
along with four other defects. Every one of them lived in the gap between "tests pass" and "code
runs" — which is the whole argument for treating an unrun pipeline as unfinished.

Bugs found, in the order the run hit them:

**1. The SEC EDGAR bulk URL was dead (404).** `edgar.py:45` and `config.py:36` both hardcoded
`https://www.sec.gov/dera/data/financial-statements/{y}q{q}.zip`. The SEC has since moved the
Financial Statement Data Sets to `/files/dera/data/financial-statement-data-sets/`. No fundamentals
had ever been downloadable. The URL existed in two places, which is how they drifted from reality
independently; `edgar.py` now reads it from `config`, restoring the single-source rule the
architecture claims. Verified live: 2026q1 downloads and parses to 600,582 facts.

**2. `pd.read_html` on a raw HTML string was removed in pandas 3.0.** `sp500_membership.py:85`
passed a string, which pandas 3 interprets as a file path — `FileNotFoundError`. The membership
scrape could not have worked on any pandas 3 install. Wrapped in `StringIO`.

**3. The Wikipedia changes table had moved to a different article.** The scraper took "the second
wikitable on *List of S&P 500 companies*"; that page now has exactly one table, because the changes
log was split out into *Historical components of the S&P 500*. Positional selection was the real
defect — the table is now located by its `Added`/`Removed` headers, and a parametrised test pins
that it is found whether it appears first, second, or alone. The new source is also better data:
407 change events back to 1976, against the old table's mid-1990s start.

**4. Scraped tickers carried wikitext debris.** Three rows came through as `ALLE |`, `JCP |`,
`ITT |` — leaked pipe characters. Left alone these become tickers yfinance cannot resolve, so
Allegion, JCPenney and ITT would have silently vanished from the universe: survivorship bias
introduced by a parsing artifact. Tickers are now extracted with `^([A-Z][A-Z0-9.\-]*)`.

**5. The writer was destroying data, and my own round-trip test could not see it.**
`ParquetWriter.write_prices()` wrote a fixed `data.parquet` per date partition, and both scripts
called it once per ticker in a loop. Every ticker overwrote the previous one's rows in every date
they shared. The first successful smoke run downloaded and cleaned 19 tickers and left **3** in the
store, 8,459 rows where there should have been ~100,000.

This is the most instructive failure of the session. `test_storage_roundtrip.py` — written
specifically to cover the storage seam, and mutation-tested — used a single ticker throughout, so
it could not express the bug. A date partition holds every ticker trading that day; testing it with
one ticker tests the one case where overwriting is indistinguishable from correctness. Fixed with a
`part_name` parameter so independent writes coexist in a partition, and the scripts now accumulate
and flush in batches (250 tickers) rather than writing per ticker. Four tests added that write more
than one ticker; reverting the writer makes two of them fail.

**Performance, found by using the thing.** With real data in place, `get_universe()` for one date
took **121 seconds** for a 239-name universe. It was `[self.get(t) for t in tickers]`, and each
`get()` ran two unbounded scans of an 8,459-partition store. Project 2 is an event-driven
backtester that calls exactly this method once per rebalance date, so at that speed a 25-year daily
backtest would take months. Fixed by batching the price fetch into one windowed SQL statement and
caching the delisting scan per instance: **121.6s → 1.36s (~90×)**. Both paths now build snapshots
through a single `_build_snapshot()`, and a test asserts batched and single-ticker results are
identical so they cannot drift.

Guarding the fix by asserting on *query count* rather than elapsed time — the count is the property
that regressed, and a timing assertion would be flaky in CI.

**Also corrected:** `config.start_year` was 2000, but the SEC datasets begin at 2009q1 (verified:
2008q1 → 404, 2009q1 → 200). The old default bought 36 quarters of guaranteed failures rather than
nine extra years of data. Now 2009.

**Hardening added alongside the fixes:**
- `--end-year`/`--end-quarter` and `--max-tickers`, because a "cheap" smoke run bounded only at the
  start is not cheap: `--start-year 2024` is ten quarters and ~25 GB. This is what made an
  iterate-and-rerun loop possible at all.
- Per-quarter error handling in the bootstrap, so one bad quarter cannot kill a multi-hour job;
  if *every* quarter fails, it now stops rather than silently building a fundamentals-free store.
- `config.require_real_user_agent()` raises instead of warning. The placeholder
  `tierzero research@example.com` has the right shape but a fake address, and the consequence of
  ignoring a warning in an hours-long job is an SEC IP ban.
- Failed EDGAR downloads clean up their partial directory.
- `PRICE_SCHEMA` is now genuinely enforced on write. `writer.py:36` had claimed this for months
  while `write_prices()` only checked that a `date` column existed. Names and order are validated
  strictly; types are cast rather than compared, because pandas picks integer and boolean widths
  based on nulls and demanding an exact dtype match would be brittle.

**Ingestion tests, 0% → 84–99%.** Every bug above lived in `ingestion/`, which had no tests at all
because every existing test starts from an already-downloaded fixture. Now 41 tests using
`responses` for the HTTP layer and a stub for yfinance (which uses `curl_cffi`, not `requests`, so
it is not mockable at the HTTP level). The highest-value assertions are the ones that pin the
things that actually broke: that the bulk URL is not the retired path, that the changes table is
found regardless of position, and that history is fetched with `auto_adjust=False`.

**Result: 56 → 113 tests, coverage 52% → 90%.** Dependencies pinned in `requirements.lock`.

**Verified end-to-end against live servers:** EDGAR 2026q1 (600,582 facts), Wikipedia membership
(407 events, 384 tickers, no malformed symbols), yfinance prices (30 tickers, 162,994 rows,
1993–2026), SEC CIK map (10,387 mappings), and `PointInTimeLookup.get('AMZN', 2024-06-14)`
returning a correct unadjusted close of 183.66 with `in_sp500=True`.

**What this run did not cover:** the full ~1,100-ticker, 2009–present bootstrap. Older EDGAR
quarters may have schema differences that one 2026 quarter cannot reveal.

**The uncomfortable finding.** yfinance returns nothing for delisted tickers — 10 of 40 in the
smoke run (Ambac, Abiomed, AK Steel, Alexion, Alpha Natural Resources, Airgas, Arconic, Activision
Blizzard, and two others). The membership timeline is survivorship-bias-free; the price data is
not. A backtest on this store silently omits roughly a quarter of the historical universe, skewed
towards the failures and takeovers that matter most. This is a partial defeat of the project's
stated purpose and cannot be fixed without a paid source (CRSP, Norgate, Sharadar). It is now the
first entry in the README's limitations section rather than a footnote.

Related and equally sharp: `close_adj` in the store is computed at write time from all splits known
then, so it embeds future information. The docstring said so; nothing else did. It now carries a
prominent README warning, since it sits in the same table as PIT-correct columns and a consumer
would reasonably assume it is safe.

---

## Design decisions and why

| Decision | Reasoning | Alternative rejected |
|---|---|---|
| `filed_date` is the sole availability gate | The SEC acceptance timestamp is when data becomes publicly observable. Period end can precede filing by 30–75 days depending on filer size and form type — e.g. Apple's FY23 Q4 period ended 2023-12-28 but the 10-Q was filed 2024-02-02, a 36-day lookahead window. | `period_of_report`, which is what naive pipelines use and is the single most common source of silent lookahead. |
| Inclusive boundary (`filed_date <= as_of`) | A filing accepted during the day is public by market close on that date. | Exclusive `<`, which would discard same-day information that was genuinely available. |
| Store unadjusted prices (`auto_adjust=False`) + separate corporate-actions log | yfinance's auto-adjust retroactively rewrites all history when a new split occurs, corrupting queries that were previously correct. Storing raw prices lets the correct split-adjusted price be computed for any `(ticker, as_of_date)` using only splits known by that date. | Storing adjusted prices, which bakes future split knowledge into past observations. |
| Track historical index membership | Backtesting "the S&P 500" on the current constituent list silently excludes every company ever removed (Lehman, Enron, Bear Stearns), which is survivorship bias in its purest form. | Current-constituent list. |
| Polars for EDGAR parsing, DuckDB for queries, pandas for small frames | A single `num.txt` runs 2–5 GB and will OOM pandas. DuckDB gives SQL with predicate pushdown directly over Hive-partitioned Parquet. | pandas end to end. |
| Atomic writes (`.tmp` + rename) | An interrupted bootstrap (this is a 4–8 hour job) must never leave a half-written Parquet file visible to a reader. | Direct writes. |
| Strict four-stage layering, each stage depending only on the previous | Keeps the PIT enforcement point in exactly one layer (`processing/`) instead of scattered across download and query code. | Mixing download and parsing. |

---

## Status against the done criterion

| Requirement | State |
|---|---|
| Daily OHLCV for a survivorship-bias-free universe | **Run 2026-08-12** — 162,994 rows, 30 tickers, 1993–2026. Universe is *not* fully survivorship-free in practice: yfinance serves no history for many delisted names (open item 2). |
| PIT fundamentals from EDGAR XBRL, gated on filing date | **Run 2026-08-12** — 600,582 facts from 2026q1. Coverage begins 2009q1; the source does not exist earlier. |
| Corporate actions (splits, dividends, delistings) | **Run 2026-08-12** — splits/dividends stored per ticker; delisting derived from last trading date. |
| Index membership history | **Run 2026-08-12** — 407 change events, 384 tickers, back to 1976, via Wikipedia. |
| Parquet storage partitioned by date, DuckDB-queryable | **Run 2026-08-12** — 8,459 date partitions, queried through `PointInTimeLookup`. |
| Unit tests proving PIT correctness | **Verified — 18/18 leakage tests pass** (113 tests total, 90% coverage) |

## Open items

1. ~~**The test suite has never been run here.**~~ **Closed 2026-08-07.** 113 passed, 90% coverage.
2. **Price history for delisted companies is missing — the most serious remaining gap.** yfinance
   returned nothing for 10 of 40 tickers in the smoke run, all delisted or acquired. Membership is
   survivorship-bias-free; prices are not, so a backtest omits roughly a quarter of the historical
   universe, biased towards failures and takeovers. No free fix exists — it needs CRSP, Norgate or
   Sharadar. Documented prominently in the README; anything built on this store inherits it.
3. **Universe is S&P 500, spec suggests Russell 3000.** Now recorded in the README as a deliberate
   scope reduction (S&P 500 membership history is free, Russell's is not) rather than left implicit.
   Revisit if a later project needs small caps.
4. **Membership source is Wikipedia.** 407 events back to 1976 — better than assumed, but still not
   authoritative, and the article layout has already changed once mid-project. Reconstructing from
   SEC 13F filings, as the spec suggests, remains unimplemented. The scraper now fails loudly on
   structural change rather than silently returning an empty timeline.
5. ~~**Uncommitted working tree.**~~ **Closed 2026-08-09** — three commits pushed.
6. ~~**README has no limitations section.**~~ **Closed 2026-08-12.**
7. ~~**Dependencies are unpinned.**~~ **Closed 2026-08-12** — `requirements.lock`.
8. ~~**Untested surface.**~~ **Closed 2026-08-12** — `ingestion/` 0% → 84–99%.
9. ~~**`PRICE_SCHEMA` is documented as enforced but isn't.**~~ **Closed 2026-08-12** — genuinely
   enforced now.
10. **The full bootstrap has still not been run.** One 2026 quarter and 40 tickers is a smoke test,
    not the real thing; ~68 quarters back to 2009q1 and ~1,100 tickers remain. Older EDGAR quarters
    may carry schema differences a single 2026 quarter cannot reveal. The per-quarter error handling
    added on 2026-08-12 means such differences will now be reported rather than fatal.
11. **`close_adj` in the store is not point-in-time correct.** Computed at write time from all
    splits known then. `PointInTimeLookup` does the right thing for `close`, and the README warns
    about it, but a raw SQL consumer reading the Parquet directly would not know. Consider either
    dropping the column from storage or renaming it to something self-evidently unsafe.
12. **`edgar_parser.py` sits at 63% coverage** — the lowest in the codebase, and it is where the
    `filed_date` PIT anchor is applied. The leakage tests cover the gating logic on synthetic data;
    the parsing of real `num.txt`/`sub.txt` shapes is thinner. Worth fixture-based tests against a
    trimmed real quarter before the full bootstrap.
