# Project 1 — Point-in-Time Equity Data Pipeline

**Tier:** 0 (Infrastructure)
**Spec:** `ResearchToDo.md` → Part 3 → Tier 0 → Project 1
**Repo:** https://github.com/fleeeeetcheeee/Point-in-time-equity-data-pipeline
**Status:** Done criterion **met**. Suite green — 56 passed, 52% coverage. Never run against real
data. See [Open items](#open-items).

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
| Daily OHLCV for a survivorship-bias-free universe | Implemented, not run |
| PIT fundamentals from EDGAR XBRL, gated on filing date | Implemented, not run |
| Corporate actions (splits, dividends, delistings) | Implemented, not run |
| Index membership history | Implemented (S&P 500 via Wikipedia), not run |
| Parquet storage partitioned by date, DuckDB-queryable | Implemented, not run |
| Unit tests proving PIT correctness | **Verified 2026-08-07 — 18/18 integration tests pass** |

## Open items

1. ~~**The test suite has never been run here.**~~ **Closed 2026-08-07.** Suite is green:
   56 passed, 0 failed, 52% coverage. Done criterion verified.
2. **The bootstrap has never been run.** All `data/` subdirectories contain only `.gitkeep`. The
   pipeline has not touched real EDGAR or yfinance data, so nothing is validated against the
   messiness of the actual sources — only against synthetic fixtures.
3. **Universe is S&P 500, spec suggests Russell 3000.** The spec names Russell 3000 historical
   constituents as the target universe; this implements S&P 500 membership. Worth either extending
   or stating explicitly as a scoped-down choice in the README.
4. **Membership source is Wikipedia.** The spec floats reconstructing membership from SEC 13F
   filings as the more rigorous route. Wikipedia's change table is serviceable but is neither
   authoritative nor guaranteed complete for the earlier part of the 2000–present range.
5. **Uncommitted working tree.** `EXPLANATION.md` untracked, `scripts/.gitkeep` deletion unstaged,
   this file untracked.
6. **README has no limitations section.** Portfolio standard (`ResearchToDo.md` Part 4) requires an
   honest-limitations section; the current README ends at design decisions. Items 2–4 above are the
   substance of it.
7. **Dependencies are unpinned.** Open lower bounds in `pyproject.toml` resolved to pandas 3.0.5 /
   numpy 2.5.1 on 2026-08-07. Add a lockfile so the "code that runs" claim is reproducible.
8. ~~**Untested surface.**~~ **Partly closed 2026-08-07** by `test_storage_roundtrip.py`: the
   processing → storage → reader → lookup seam is now covered (writer 0% → 87%, lookup 40% → 92%).
   Still open: `ingestion/` remains at 0%, since nothing there runs without network I/O. Closing it
   needs recorded HTTP fixtures (e.g. `responses`/`vcrpy`) for the EDGAR, yfinance, and Wikipedia
   clients — worth doing before item 2, so bootstrap failures surface as test failures rather than
   as a crash hours into a run.
9. **`PRICE_SCHEMA` is documented as enforced but isn't.** `writer.py:36` states the schema is
   "Enforced on write so downstream consumers can rely on column presence and types," but
   `write_prices()` only checks that a `date` column exists — it never validates names or dtypes
   against `PRICE_SCHEMA`. The new round-trip test asserts conformance from the outside, so the
   practical risk is covered, but either the writer should validate or the comment should be
   corrected.
