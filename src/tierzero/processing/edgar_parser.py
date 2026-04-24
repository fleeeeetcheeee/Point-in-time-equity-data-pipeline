"""
SEC EDGAR XBRL bulk dataset parser.

Point-in-time correctness is enforced here at the source:

  filed_date  ← sub.txt "filed" column (YYYYMMDD int)  ← USE THIS for PIT
  period_date ← sub.txt "period" column (YYYYMMDD int) ← store for reference only

A query for (ticker, 2024-01-15) must only see filings where
filed_date <= 2024-01-15, regardless of what period they cover.
Apple's fiscal Q1 2024 (period ending 2023-12-28) was not filed until
2024-02-02 — a January query must not see it.

Sub.txt schema (selected columns):
  adsh      — accession number, unique filing ID  e.g. "0000320193-24-000123"
  cik       — SEC Central Index Key (integer)
  name      — company name at time of filing
  form      — form type: 10-K, 10-Q, 10-K/A, 10-Q/A, 8-K, ...
  period    — period of report as YYYYMMDD integer  ← do NOT use for PIT
  filed     — filing date as YYYYMMDD integer       ← PIT anchor
  prevrpt   — 1 if this supersedes a prior filing for the same period

Num.txt schema (selected columns):
  adsh      — foreign key to sub.txt
  tag       — XBRL concept name  e.g. "Assets", "EarningsPerShareBasic"
  ddate     — period end date as YYYYMMDD integer   ← do NOT use for PIT
  qtrs      — quarters covered: 0=instant/balance-sheet, 1=one quarter, 4=annual
  uom       — unit of measure: "USD", "shares", "pure", ...
  value     — numeric value
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

log = logging.getLogger(__name__)

# Standard US-GAAP tags we extract. Restricting the set before the join
# keeps memory usage manageable — a single num.txt can be 2-5 GB uncompressed.
CORE_TAGS: frozenset[str] = frozenset(
    {
        # --- Income statement ---
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "GrossProfit",
        "OperatingIncomeLoss",
        "NetIncomeLoss",
        "NetIncomeLossAvailableToCommonStockholdersBasic",
        "EarningsPerShareBasic",
        "EarningsPerShareDiluted",
        "WeightedAverageNumberOfSharesOutstandingBasic",
        "WeightedAverageNumberOfDilutedSharesOutstanding",
        # --- Balance sheet — assets ---
        "Assets",
        "AssetsCurrent",
        "CashAndCashEquivalentsAtCarryingValue",
        "ShortTermInvestments",
        "AccountsReceivableNetCurrent",
        "InventoryNet",
        "AssetsNoncurrent",
        "PropertyPlantAndEquipmentNet",
        "Goodwill",
        "IntangibleAssetsNetExcludingGoodwill",
        # --- Balance sheet — liabilities & equity ---
        "Liabilities",
        "LiabilitiesCurrent",
        "AccountsPayableCurrent",
        "LongTermDebt",
        "LongTermDebtNoncurrent",
        "StockholdersEquity",
        "RetainedEarningsAccumulatedDeficit",
        # --- Cash flow ---
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInInvestingActivities",
        "NetCashProvidedByUsedInFinancingActivities",
        "DepreciationDepletionAndAmortization",
        "PaymentsToAcquirePropertyPlantAndEquipment",
        # --- Share count ---
        "CommonStockSharesOutstanding",
        "CommonStockSharesIssued",
    }
)

_ANNUAL_FORMS = frozenset({"10-K", "10-K/A"})
_QUARTERLY_FORMS = frozenset({"10-Q", "10-Q/A"})
_FILING_FORMS = _ANNUAL_FORMS | _QUARTERLY_FORMS


class EdgarParser:
    """
    Parses one or more quarterly EDGAR XBRL directories into a Polars DataFrame.

    Output schema (one row per XBRL fact as originally filed):
      adsh          Utf8    — accession number
      cik           Int64   — company CIK
      name          Utf8    — company name at filing time
      filed_date    Date    — THE point-in-time date (from sub.filed)
      period_date   Date    — period of report (for reference only)
      form          Utf8    — 10-K / 10-Q / 10-K/A / 10-Q/A
      is_amendment  Boolean — True for /A forms
      tag           Utf8    — XBRL concept
      qtrs          Int8    — 0=balance-sheet instant, 1=quarterly, 4=annual
      uom           Utf8    — unit of measure
      value         Float64 — numeric value

    Usage::

        parser = EdgarParser()
        df = parser.parse_quarter(Path("data/raw/edgar/2024q1"))
        parser.parse_all_quarters(raw_dir, processed_dir)
    """

    def parse_quarter(
        self,
        quarter_dir: Path,
        forms: frozenset[str] = _FILING_FORMS,
        tags: frozenset[str] = CORE_TAGS,
    ) -> pl.DataFrame:
        """
        Parse one quarterly EDGAR dataset directory.

        Filters to the specified form types and XBRL tags *before* joining
        sub.txt and num.txt to keep peak memory usage low.
        """
        sub_path = quarter_dir / "sub.txt"
        num_path = quarter_dir / "num.txt"

        if not sub_path.exists() or not num_path.exists():
            raise FileNotFoundError(
                f"Expected sub.txt and num.txt in {quarter_dir}"
            )

        sub = self._load_sub(sub_path, forms)
        num = self._load_num(num_path, tags)

        facts = num.join(sub, on="adsh", how="inner")

        return facts.select(
            [
                "adsh",
                "cik",
                "name",
                "filed_date",
                "period_date",
                "form",
                "is_amendment",
                "tag",
                "qtrs",
                "uom",
                "value",
            ]
        )

    def parse_all_quarters(
        self,
        raw_dir: Path,
        output_dir: Path,
        **kwargs,
    ) -> None:
        """
        Parse every downloaded quarterly directory and write one Parquet per quarter.

        Skips quarters whose output file already exists (idempotent).
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        quarter_dirs = sorted(
            d for d in raw_dir.iterdir() if d.is_dir() and not d.name.startswith("_")
        )

        for qdir in quarter_dirs:
            out_path = output_dir / f"{qdir.name}.parquet"
            if out_path.exists():
                log.info("Skipping %s (already parsed).", qdir.name)
                continue

            log.info("Parsing EDGAR %s ...", qdir.name)
            try:
                df = self.parse_quarter(qdir, **kwargs)
                tmp = output_dir / f"{qdir.name}.tmp.parquet"
                df.write_parquet(tmp, compression="zstd")
                tmp.rename(out_path)
                log.info("Wrote %d facts to %s", len(df), out_path)
            except Exception as exc:
                log.error("Failed to parse %s: %s", qdir.name, exc)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _load_sub(path: Path, forms: frozenset[str]) -> pl.DataFrame:
        sub = pl.read_csv(
            path,
            separator="\t",
            columns=["adsh", "cik", "name", "form", "period", "filed", "prevrpt"],
            schema_overrides={
                "adsh": pl.Utf8,
                "cik": pl.Int64,
                "name": pl.Utf8,
                "form": pl.Utf8,
                "period": pl.Int64,
                "filed": pl.Int64,
                "prevrpt": pl.Int8,
            },
            ignore_errors=True,
            truncate_ragged_lines=True,
        ).filter(pl.col("form").is_in(list(forms)))

        sub = sub.with_columns(
            [
                _int_yyyymmdd_to_date("filed").alias("filed_date"),
                _int_yyyymmdd_to_date("period").alias("period_date"),
                pl.col("form").str.ends_with("/A").alias("is_amendment"),
            ]
        ).drop(["filed", "period", "prevrpt"])

        return sub

    @staticmethod
    def _load_num(path: Path, tags: frozenset[str]) -> pl.DataFrame:
        return pl.read_csv(
            path,
            separator="\t",
            columns=["adsh", "tag", "ddate", "qtrs", "uom", "value"],
            schema_overrides={
                "adsh": pl.Utf8,
                "tag": pl.Utf8,
                "ddate": pl.Int64,
                "qtrs": pl.Int8,
                "uom": pl.Utf8,
                "value": pl.Float64,
            },
            ignore_errors=True,
            truncate_ragged_lines=True,
        ).filter(pl.col("tag").is_in(list(tags))).drop("ddate")


def _int_yyyymmdd_to_date(col: str) -> pl.Expr:
    """Convert an integer YYYYMMDD column to a Polars Date type."""
    return (
        pl.col(col)
        .cast(pl.Utf8)
        .str.strptime(pl.Date, "%Y%m%d", strict=False)
    )
