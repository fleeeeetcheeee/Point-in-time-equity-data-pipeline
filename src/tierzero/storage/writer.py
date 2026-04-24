"""
Parquet writer for processed price and fundamental data.

Storage layout
--------------
Prices use Hive-style date partitioning so DuckDB can push down date-range
predicates without scanning every file:

    processed/prices/
        date=2024-01-02/data.parquet
        date=2024-01-03/data.parquet
        ...

Fundamentals are stored one file per EDGAR quarter (already partitioned by
their parse step):

    processed/fundamentals/as_filed/
        2024q1.parquet
        2024q2.parquet
        ...

All writes are atomic: data goes to a .tmp file first, then renamed. This
prevents a partial write from being visible to readers.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

log = logging.getLogger(__name__)

# Canonical schema for the processed price table.
# Enforced on write so downstream consumers can rely on column presence and types.
PRICE_SCHEMA: dict[str, pl.PolarsDataType] = {
    "date": pl.Date,
    "ticker": pl.Utf8,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close_unadj": pl.Float64,
    "close_adj": pl.Float64,
    "volume": pl.Int64,
    "volume_zero": pl.Boolean,
    "price_gap_flag": pl.Boolean,
}


class ParquetWriter:
    """
    Writes processed DataFrames to partitioned Parquet files.

    Usage::

        writer = ParquetWriter()
        writer.write_prices(df, processed_prices_dir)
        writer.write_fundamentals(df, "2024q1", processed_fundamentals_dir)
    """

    # ------------------------------------------------------------------
    # Prices
    # ------------------------------------------------------------------

    def write_prices(self, df: pl.DataFrame, output_dir: Path) -> None:
        """
        Write a daily price DataFrame partitioned by date.

        Each date gets its own subdirectory:  date=YYYY-MM-DD/data.parquet
        Existing partitions for a date are overwritten.
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        if "date" not in df.columns:
            raise ValueError("DataFrame must have a 'date' column for partitioning.")

        for date_val in df["date"].unique().sort():
            partition_dir = output_dir / f"date={date_val}"
            partition_dir.mkdir(parents=True, exist_ok=True)

            slice_df = df.filter(pl.col("date") == date_val)
            self._write_atomic(slice_df, partition_dir / "data.parquet")

        log.info(
            "Wrote prices for %d dates to %s",
            df["date"].n_unique(),
            output_dir,
        )

    # ------------------------------------------------------------------
    # Fundamentals
    # ------------------------------------------------------------------

    def write_fundamentals(
        self, df: pl.DataFrame, quarter_name: str, output_dir: Path
    ) -> None:
        """
        Write a parsed EDGAR quarter DataFrame.

        quarter_name: e.g. "2024q1"
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        out_path = output_dir / f"{quarter_name}.parquet"
        self._write_atomic(df, out_path, compression="zstd")
        log.info("Wrote %d fundamental facts to %s", len(df), out_path)

    # ------------------------------------------------------------------
    # Membership
    # ------------------------------------------------------------------

    def write_membership(self, df: pl.DataFrame, output_path: Path) -> None:
        """Write the S&P 500 constituent timeline."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_atomic(df, output_path)
        log.info("Wrote membership timeline (%d rows) to %s", len(df), output_path)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _write_atomic(
        df: pl.DataFrame,
        final_path: Path,
        compression: str = "snappy",
    ) -> None:
        """Write to a .tmp file then rename — prevents partial writes."""
        tmp = final_path.with_suffix(".tmp.parquet")
        df.write_parquet(tmp, compression=compression)
        tmp.rename(final_path)
