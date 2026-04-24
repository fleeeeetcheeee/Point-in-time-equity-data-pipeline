"""
Unit tests for EdgarParser.

Tests use temporary directories with minimal sub.txt / num.txt fixture files
that mirror the exact TSV format of the real SEC EDGAR data. No network calls.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from tierzero.processing.edgar_parser import EdgarParser, CORE_TAGS


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _write_quarter(tmp_path: Path, sub_rows: list[str], num_rows: list[str]) -> Path:
    """Write minimal sub.txt and num.txt files to a temp directory."""
    sub_header = "adsh\tcik\tname\tform\tperiod\tfiled\tprevrpt\n"
    num_header = "adsh\ttag\tddate\tqtrs\tuom\tvalue\n"

    (tmp_path / "sub.txt").write_text(sub_header + "\n".join(sub_rows) + "\n")
    (tmp_path / "num.txt").write_text(num_header + "\n".join(num_rows) + "\n")
    return tmp_path


# ------------------------------------------------------------------
# Date conversion
# ------------------------------------------------------------------

class TestDateConversion:
    def test_yyyymmdd_filed_date_parsed_correctly(self, tmp_path):
        _write_quarter(
            tmp_path,
            sub_rows=["0000320193-24-000010\t320193\tApple Inc.\t10-Q\t20231230\t20240202\t0"],
            num_rows=["0000320193-24-000010\tAssets\t20231230\t0\tUSD\t352583000000"],
        )
        df = EdgarParser().parse_quarter(tmp_path, tags=frozenset({"Assets"}))

        assert df.height == 1
        row = df.row(0, named=True)
        assert row["filed_date"] == date(2024, 2, 2)
        assert row["period_date"] == date(2023, 12, 30)

    def test_filed_date_after_period_date(self, tmp_path):
        """A core PIT sanity check: filings are always after their period end."""
        _write_quarter(
            tmp_path,
            sub_rows=["0000320193-24-000010\t320193\tApple Inc.\t10-Q\t20231230\t20240202\t0"],
            num_rows=["0000320193-24-000010\tAssets\t20231230\t0\tUSD\t352583000000"],
        )
        df = EdgarParser().parse_quarter(tmp_path, tags=frozenset({"Assets"}))
        row = df.row(0, named=True)
        assert row["filed_date"] > row["period_date"], (
            "filed_date must be after period_date — "
            "this is the fundamental PIT correctness invariant"
        )


# ------------------------------------------------------------------
# Amendment detection
# ------------------------------------------------------------------

class TestAmendmentFlag:
    def test_10k_a_flagged_as_amendment(self, tmp_path):
        _write_quarter(
            tmp_path,
            sub_rows=["0000012345-24-000001\t12345\tTest Corp\t10-K/A\t20231231\t20240315\t1"],
            num_rows=["0000012345-24-000001\tNetIncomeLoss\t20231231\t4\tUSD\t950000"],
        )
        df = EdgarParser().parse_quarter(tmp_path, tags=frozenset({"NetIncomeLoss"}))
        assert df["is_amendment"][0] is True

    def test_10q_not_flagged_as_amendment(self, tmp_path):
        _write_quarter(
            tmp_path,
            sub_rows=["0000012345-24-000002\t12345\tTest Corp\t10-Q\t20231231\t20240202\t0"],
            num_rows=["0000012345-24-000002\tNetIncomeLoss\t20231231\t1\tUSD\t250000"],
        )
        df = EdgarParser().parse_quarter(tmp_path, tags=frozenset({"NetIncomeLoss"}))
        assert df["is_amendment"][0] is False


# ------------------------------------------------------------------
# Form filtering
# ------------------------------------------------------------------

class TestFormFiltering:
    def test_8k_excluded(self, tmp_path):
        """8-K forms must not appear in fundamental output."""
        _write_quarter(
            tmp_path,
            sub_rows=[
                "AAPL-8K\t320193\tApple Inc.\t8-K\t20240131\t20240131\t0",
                "AAPL-10Q\t320193\tApple Inc.\t10-Q\t20231230\t20240202\t0",
            ],
            num_rows=[
                "AAPL-8K\tAssets\t20240131\t0\tUSD\t100000",
                "AAPL-10Q\tAssets\t20231230\t0\tUSD\t352583000000",
            ],
        )
        df = EdgarParser().parse_quarter(tmp_path, tags=frozenset({"Assets"}))
        assert df.height == 1
        assert df["adsh"][0] == "AAPL-10Q"

    def test_custom_form_filter(self, tmp_path):
        """Caller can restrict to only annual filings."""
        _write_quarter(
            tmp_path,
            sub_rows=[
                "ANNUAL\t12345\tCo\t10-K\t20231231\t20240215\t0",
                "QUARTER\t12345\tCo\t10-Q\t20230930\t20231103\t0",
            ],
            num_rows=[
                "ANNUAL\tAssets\t20231231\t0\tUSD\t1000",
                "QUARTER\tAssets\t20230930\t0\tUSD\t950",
            ],
        )
        df = EdgarParser().parse_quarter(
            tmp_path,
            forms=frozenset({"10-K"}),
            tags=frozenset({"Assets"}),
        )
        assert df.height == 1
        assert df["form"][0] == "10-K"


# ------------------------------------------------------------------
# Tag filtering
# ------------------------------------------------------------------

class TestTagFiltering:
    def test_non_core_tag_excluded(self, tmp_path):
        """Tags not in CORE_TAGS must be filtered out."""
        _write_quarter(
            tmp_path,
            sub_rows=["ADSH001\t12345\tCo\t10-Q\t20231231\t20240101\t0"],
            num_rows=[
                "ADSH001\tAssets\t20231231\t0\tUSD\t500000",
                "ADSH001\tSomeCustomTag\t20231231\t0\tUSD\t999",
            ],
        )
        df = EdgarParser().parse_quarter(tmp_path)
        tags_returned = set(df["tag"].to_list())
        assert "SomeCustomTag" not in tags_returned
        assert "Assets" in tags_returned


# ------------------------------------------------------------------
# Output schema
# ------------------------------------------------------------------

class TestOutputSchema:
    EXPECTED_COLUMNS = {
        "adsh", "cik", "name", "filed_date", "period_date",
        "form", "is_amendment", "tag", "qtrs", "uom", "value",
    }

    def test_output_has_all_required_columns(self, tmp_path):
        _write_quarter(
            tmp_path,
            sub_rows=["ADSH001\t12345\tCo\t10-Q\t20231231\t20240101\t0"],
            num_rows=["ADSH001\tAssets\t20231231\t0\tUSD\t500000"],
        )
        df = EdgarParser().parse_quarter(tmp_path, tags=frozenset({"Assets"}))
        assert self.EXPECTED_COLUMNS.issubset(set(df.columns))
