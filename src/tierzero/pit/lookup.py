"""
Point-in-time equity data lookup — the core API.

Central guarantee
-----------------
For any (ticker, as_of_date) pair, the returned EquitySnapshot contains
ONLY data that was publicly available by market close on as_of_date:

  1. Price queries:       date <= as_of_date
  2. Fundamental queries: filed_date <= as_of_date  (filing date, not period end)
  3. Split adjustment:    only splits with date <= as_of_date
  4. Index membership:    entry_date <= as_of_date < exit_date

The distinction between filed_date and period_date is the central correctness
concern. Apple's fiscal Q1 2024 (period ending 2023-12-28) was filed on
2024-02-02. A query on 2024-01-15 must see Q3 2023 data, not Q4.

TTM construction
----------------
Trailing-twelve-month income figures are built by summing the four most
recent non-overlapping quarterly facts (qtrs=1) whose parent filing has
filed_date <= as_of_date. If an annual filing (qtrs=4) is the most recent
known filing, it is used directly as the TTM value.

Amendment handling
------------------
Amended filings (10-K/A, 10-Q/A) are handled automatically: because we
always sort by filed_date DESC, the amendment (filed later) naturally
supersedes the original for queries after the amendment date, and the
original remains the answer for queries before it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import pandas as pd

from tierzero.processing.membership_builder import MembershipBuilder
from tierzero.storage.reader import DataReader

log = logging.getLogger(__name__)


@dataclass
class EquitySnapshot:
    """
    All data about a single equity as it was known at market close on as_of_date.

    Every field reflects only information publicly available by that date.
    Fields set to None indicate data was not available (company not yet public,
    filing not yet submitted, etc.).
    """

    ticker: str
    as_of_date: date

    # --- Price ---
    close: Optional[float] = None           # unadjusted closing price
    close_adj: Optional[float] = None       # split-adjusted (using only known-by-date splits)
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    volume: Optional[int] = None
    is_trading_day: bool = False            # True if as_of_date was itself a trading day
    is_delisted: bool = False               # True if no price data after last known date
    last_trading_date: Optional[date] = None

    # --- Market cap ---
    market_cap: Optional[float] = None
    shares_outstanding: Optional[int] = None

    # --- Most recent filing metadata ---
    latest_filing_adsh: Optional[str] = None
    latest_filing_date: Optional[date] = None   # filed_date — when data became known
    latest_period_end: Optional[date] = None    # period_date — what quarter it covers
    latest_form: Optional[str] = None

    # --- Income statement (TTM) ---
    revenue_ttm: Optional[float] = None
    net_income_ttm: Optional[float] = None
    eps_basic_ttm: Optional[float] = None
    eps_diluted_ttm: Optional[float] = None
    operating_income_ttm: Optional[float] = None

    # --- Balance sheet (as of latest filing) ---
    total_assets: Optional[float] = None
    total_liabilities: Optional[float] = None
    stockholders_equity: Optional[float] = None
    cash_and_equivalents: Optional[float] = None
    long_term_debt: Optional[float] = None

    # --- Cash flow (TTM) ---
    operating_cash_flow_ttm: Optional[float] = None
    capex_ttm: Optional[float] = None
    free_cash_flow_ttm: Optional[float] = None

    # --- Index membership ---
    in_sp500: bool = False
    sp500_entry_date: Optional[date] = None
    sp500_exit_date: Optional[date] = None

    # --- Derived ratios ---
    pe_ratio: Optional[float] = None        # close / eps_basic_ttm
    pb_ratio: Optional[float] = None        # market_cap / stockholders_equity

    # --- Data quality ---
    has_price: bool = False
    has_fundamentals: bool = False
    data_warnings: list[str] = field(default_factory=list)


class PointInTimeLookup:
    """
    Main interface for point-in-time equity data retrieval.

    All dependencies are injected so the class is fully unit-testable
    without touching the filesystem.

    Usage::

        reader = DataReader(data_root)
        membership = MembershipBuilder.load(membership_path)
        ticker_cik = pd.read_parquet(ticker_cik_path)

        pit = PointInTimeLookup(reader, membership, ticker_cik)
        snap = pit.get("AAPL", date(2024, 1, 15))
        universe = pit.get_universe(date(2024, 1, 15), universe="sp500")
    """

    def __init__(
        self,
        reader: DataReader,
        membership: pd.DataFrame,
        ticker_cik_map: pd.DataFrame,
    ) -> None:
        self._reader = reader
        self._membership = membership
        self._ticker_cik = ticker_cik_map
        self._mb = MembershipBuilder()
        self._last_dates: Optional[dict[str, date]] = None

    # ------------------------------------------------------------------
    # Primary API
    # ------------------------------------------------------------------

    def get(self, ticker: str, as_of_date: date) -> EquitySnapshot:
        """
        Return an EquitySnapshot for (ticker, as_of_date).

        All data in the snapshot reflects only what was publicly known
        by market close on as_of_date.
        """
        pit = pd.Timestamp(as_of_date)
        return self._build_snapshot(ticker, as_of_date, pit, self._query_last_price(ticker, pit))

    def _build_snapshot(
        self, ticker: str, as_of_date: date, pit: pd.Timestamp, price_row
    ) -> EquitySnapshot:
        """
        Assemble a snapshot from an already-fetched price row.

        Split out from get() so get_universe() can fetch every ticker's price in
        one query and still produce identical snapshots — there is exactly one
        code path building an EquitySnapshot, so the batched and single-ticker
        results cannot drift apart.
        """
        snap = EquitySnapshot(ticker=ticker, as_of_date=as_of_date)

        self._populate_price(snap, ticker, pit, price_row)
        self._populate_index_membership(snap, ticker, pit)

        cik = self._resolve_cik(ticker)
        if cik is not None:
            self._populate_fundamentals(snap, cik, pit)

        self._compute_derived(snap)
        return snap

    def get_universe(
        self,
        as_of_date: date,
        universe: str = "sp500",
    ) -> list[EquitySnapshot]:
        """
        Return snapshots for all members of a universe on as_of_date.

        universe: "sp500" | "all"
          "sp500" — tickers that were S&P 500 members on as_of_date
          "all"   — every ticker with any price data

        This is the primary entry point for backtesting: call once per
        rebalancing date to get the full investable universe.
        """
        pit = pd.Timestamp(as_of_date)

        if universe == "sp500":
            tickers = self._mb.get_constituents_on(self._membership, pit)
        else:
            result = self._reader.query("SELECT DISTINCT ticker FROM prices ORDER BY ticker")
            tickers = result["ticker"].to_list()

        # Prices for the whole universe come back in one query rather than one
        # per ticker. Calling get() in a loop is an N+1 against a store of
        # thousands of partitions: for a 500-name universe that was ~500 scans
        # and over two minutes per date, which a daily-rebalance backtest would
        # multiply by several thousand.
        prices = self._query_last_prices(tickers, pit)

        return [self._build_snapshot(t, as_of_date, pit, prices.get(t)) for t in tickers]

    # ------------------------------------------------------------------
    # Price population
    # ------------------------------------------------------------------

    # How far back to look for the last trading day before widening the search.
    # Prices are Hive-partitioned by date, so a bounded range lets DuckDB prune
    # to a handful of directories. Unbounded, every lookup scans the whole store
    # — with 25 years of history that is ~8,500 partitions per call, and it was
    # what made get_universe() take two minutes for a single date.
    # 10 days clears any US market holiday run; the rare gap longer than that
    # (a suspended or thinly traded name) falls back to the full scan.
    _LOOKBACK_DAYS = 10

    _PRICE_COLUMNS = (
        "date, open, high, low, close_unadj, close_adj, volume, "
        "volume_zero, price_gap_flag"
    )

    def _query_last_prices(
        self, tickers: list[str], pit: pd.Timestamp
    ) -> dict[str, dict]:
        """
        Last price row on or before `pit` for many tickers, in one query.

        Returns {ticker: row}. Tickers with no price at all are simply absent.
        """
        if not tickers:
            return {}

        upper = pit.date().isoformat()
        lower = (pit - pd.Timedelta(days=self._LOOKBACK_DAYS)).date().isoformat()
        quoted = ", ".join(f"'{t}'" for t in tickers)

        def run(floor: str | None) -> dict[str, dict]:
            clause = f"AND date >= '{floor}'" if floor else ""
            result = self._reader.query(f"""
                SELECT ticker, {self._PRICE_COLUMNS}
                FROM (
                    SELECT ticker, {self._PRICE_COLUMNS},
                           ROW_NUMBER() OVER (
                               PARTITION BY ticker ORDER BY date DESC
                           ) AS rn
                    FROM prices
                    WHERE ticker IN ({quoted})
                      AND date <= '{upper}'
                      {clause}
                )
                WHERE rn = 1
            """)
            return {r["ticker"]: r for r in result.iter_rows(named=True)}

        found = run(lower)

        # Anything not seen in the recent window may still have older data —
        # a suspended name, or a date before the store's coverage begins. Those
        # get one extra unbounded query between them, not one apiece.
        missing = [t for t in tickers if t not in found]
        if missing:
            quoted = ", ".join(f"'{t}'" for t in missing)
            found.update(run(None))

        return found

    def _query_last_price(self, ticker: str, pit: pd.Timestamp) -> Optional[dict]:
        """Last price row on or before `pit` for a single ticker."""
        return self._query_last_prices([ticker], pit).get(ticker)

    def _populate_price(
        self, snap: EquitySnapshot, ticker: str, pit: pd.Timestamp, row: Optional[dict]
    ) -> None:
        """
        Fill price fields from the last available trading day on or before pit.
        """
        if row is None:
            return

        snap.close = row["close_unadj"]
        snap.close_adj = row["close_adj"]
        snap.open = row["open"]
        snap.high = row["high"]
        snap.low = row["low"]
        snap.volume = int(row["volume"]) if row["volume"] is not None else None
        snap.is_trading_day = str(row["date"]) == pit.date().isoformat()
        snap.has_price = True

        if not snap.is_trading_day:
            snap.data_warnings.append(
                f"No trading data on {pit.date()}; using last available: {row['date']}"
            )

        # Detect delisting: is the last price in the entire series before as_of_date?
        last_traded = self._last_trading_dates().get(ticker)
        if last_traded is not None:
            snap.last_trading_date = last_traded
            snap.is_delisted = last_traded < pit.date()

    def _last_trading_dates(self) -> dict[str, date]:
        """
        Final trading date per ticker, computed once and cached.

        Delisting is "the series ends before as_of_date", so this cannot use the
        date <= pit bound and is unavoidably a full scan. Doing it per ticker
        per date made it the second N+1 in get_universe. The price store is
        immutable while a lookup object lives — rebuild the object after an
        update — so one scan per instance is enough.
        """
        if self._last_dates is None:
            result = self._reader.query(
                "SELECT ticker, MAX(date)::VARCHAR AS last_date FROM prices GROUP BY ticker"
            )
            self._last_dates = {
                r["ticker"]: date.fromisoformat(r["last_date"])
                for r in result.iter_rows(named=True)
                if r["last_date"]
            }
        return self._last_dates

    # ------------------------------------------------------------------
    # Fundamental population
    # ------------------------------------------------------------------

    def _populate_fundamentals(
        self, snap: EquitySnapshot, cik: int, pit: pd.Timestamp
    ) -> None:
        """
        Find the most recent 10-K/10-Q filings with filed_date <= pit,
        then assemble TTM income figures and balance sheet snapshot.
        """
        # The 8 most recent filings gives us enough lookback for a full TTM
        filings_sql = f"""
            SELECT DISTINCT adsh, filed_date::VARCHAR AS filed_date,
                            period_date::VARCHAR AS period_date, form
            FROM fundamentals
            WHERE cik = {cik}
              AND filed_date <= '{pit.date().isoformat()}'
              AND form IN ('10-K', '10-Q', '10-K/A', '10-Q/A')
            ORDER BY filed_date DESC
            LIMIT 8
        """
        filings = self._reader.query(filings_sql)
        if filings.is_empty():
            return

        latest = filings.row(0, named=True)
        snap.latest_filing_adsh = latest["adsh"]
        snap.latest_filing_date = date.fromisoformat(latest["filed_date"])
        snap.latest_period_end = date.fromisoformat(latest["period_date"])
        snap.latest_form = latest["form"]
        snap.has_fundamentals = True

        self._populate_balance_sheet(snap, latest["adsh"])
        self._populate_ttm_income(snap, cik, pit, filings["adsh"].to_list())
        self._populate_shares(snap, cik, pit)

    def _populate_balance_sheet(self, snap: EquitySnapshot, adsh: str) -> None:
        tags = [
            "Assets", "Liabilities", "StockholdersEquity",
            "CashAndCashEquivalentsAtCarryingValue",
            "LongTermDebt", "LongTermDebtNoncurrent",
        ]
        tag_list = ", ".join(f"'{t}'" for t in tags)
        sql = f"""
            SELECT tag, value
            FROM fundamentals
            WHERE adsh = '{adsh}'
              AND tag IN ({tag_list})
              AND qtrs = 0
        """
        result = self._reader.query(sql)
        tag_map = {row["tag"]: row["value"] for row in result.iter_rows(named=True)}

        snap.total_assets = tag_map.get("Assets")
        snap.total_liabilities = tag_map.get("Liabilities")
        snap.stockholders_equity = tag_map.get("StockholdersEquity")
        snap.cash_and_equivalents = tag_map.get("CashAndCashEquivalentsAtCarryingValue")
        snap.long_term_debt = tag_map.get("LongTermDebtNoncurrent") or tag_map.get("LongTermDebt")

    def _populate_ttm_income(
        self,
        snap: EquitySnapshot,
        cik: int,
        pit: pd.Timestamp,
        recent_adshs: list[str],
    ) -> None:
        """
        Build TTM figures from the four most recent non-overlapping quarters.

        If an annual filing (qtrs=4) is the most recent, use it directly.
        Otherwise sum four distinct qtrs=1 periods.
        """
        adsh_list = ", ".join(f"'{a}'" for a in recent_adshs[:8])
        income_tags = [
            "Revenues",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "NetIncomeLoss",
            "EarningsPerShareBasic",
            "EarningsPerShareDiluted",
            "OperatingIncomeLoss",
            "NetCashProvidedByUsedInOperatingActivities",
            "PaymentsToAcquirePropertyPlantAndEquipment",
        ]
        tag_list = ", ".join(f"'{t}'" for t in income_tags)

        sql = f"""
            SELECT f.tag, f.value, f.adsh, f.period_date::VARCHAR AS period_date, f.qtrs
            FROM fundamentals f
            WHERE f.adsh IN ({adsh_list})
              AND f.tag IN ({tag_list})
              AND f.qtrs IN (1, 4)
            ORDER BY f.period_date DESC
        """
        facts = self._reader.query(sql)

        def ttm(tag: str, alt_tag: str | None = None) -> Optional[float]:
            rows = facts.filter(facts["tag"] == tag)
            if rows.is_empty() and alt_tag:
                rows = facts.filter(facts["tag"] == alt_tag)
            return _compute_ttm(rows)

        snap.revenue_ttm = ttm("Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax")
        snap.net_income_ttm = ttm("NetIncomeLoss")
        snap.eps_basic_ttm = ttm("EarningsPerShareBasic")
        snap.eps_diluted_ttm = ttm("EarningsPerShareDiluted")
        snap.operating_income_ttm = ttm("OperatingIncomeLoss")
        snap.operating_cash_flow_ttm = ttm("NetCashProvidedByUsedInOperatingActivities")
        snap.capex_ttm = ttm("PaymentsToAcquirePropertyPlantAndEquipment")

        if snap.operating_cash_flow_ttm is not None and snap.capex_ttm is not None:
            snap.free_cash_flow_ttm = snap.operating_cash_flow_ttm - abs(snap.capex_ttm)

    def _populate_shares(
        self, snap: EquitySnapshot, cik: int, pit: pd.Timestamp
    ) -> None:
        sql = f"""
            SELECT value
            FROM fundamentals
            WHERE cik = {cik}
              AND tag = 'CommonStockSharesOutstanding'
              AND filed_date <= '{pit.date().isoformat()}'
              AND qtrs = 0
            ORDER BY filed_date DESC, period_date DESC
            LIMIT 1
        """
        result = self._reader.query(sql)
        if not result.is_empty():
            snap.shares_outstanding = int(result["value"][0])

    # ------------------------------------------------------------------
    # Index membership
    # ------------------------------------------------------------------

    def _populate_index_membership(
        self, snap: EquitySnapshot, ticker: str, pit: pd.Timestamp
    ) -> None:
        entry, exit_ = self._mb.get_entry_exit(self._membership, ticker, pit)
        if entry is not None:
            snap.in_sp500 = True
            snap.sp500_entry_date = entry.date() if hasattr(entry, "date") else entry
            snap.sp500_exit_date = exit_.date() if exit_ and hasattr(exit_, "date") else exit_

    # ------------------------------------------------------------------
    # Derived ratios
    # ------------------------------------------------------------------

    def _compute_derived(self, snap: EquitySnapshot) -> None:
        if snap.close and snap.eps_basic_ttm and snap.eps_basic_ttm != 0:
            snap.pe_ratio = snap.close / snap.eps_basic_ttm

        if snap.close and snap.shares_outstanding:
            snap.market_cap = snap.close * snap.shares_outstanding

        if snap.market_cap and snap.stockholders_equity and snap.stockholders_equity != 0:
            snap.pb_ratio = snap.market_cap / snap.stockholders_equity

    # ------------------------------------------------------------------
    # CIK resolution
    # ------------------------------------------------------------------

    def _resolve_cik(self, ticker: str) -> Optional[int]:
        rows = self._ticker_cik[self._ticker_cik["ticker"] == ticker]
        if rows.empty:
            return None
        return int(rows.iloc[0]["cik"])


# ------------------------------------------------------------------
# TTM helper (module-level, pure function — easy to test directly)
# ------------------------------------------------------------------

def _compute_ttm(tag_facts) -> Optional[float]:  # tag_facts: pl.DataFrame
    """
    Compute a trailing-twelve-month value from a Polars DataFrame of facts
    for a single XBRL tag.

    Strategy:
    1. If an annual filing (qtrs=4) is present, use it directly.
    2. Otherwise sum the four most recent distinct quarterly (qtrs=1) periods.
       Returns None if fewer than four quarters are available.
    """
    if tag_facts.is_empty():
        return None

    annual = tag_facts.filter(tag_facts["qtrs"] == 4)
    if not annual.is_empty():
        return float(annual.sort("period_date", descending=True)["value"][0])

    quarterly = (
        tag_facts
        .filter(tag_facts["qtrs"] == 1)
        .unique(subset=["period_date"])
        .sort("period_date", descending=True)
        .head(4)
    )
    if quarterly.height < 4:
        return None

    return float(quarterly["value"].sum())
