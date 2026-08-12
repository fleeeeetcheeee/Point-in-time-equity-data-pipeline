"""
Tests for the S&P 500 membership scraper.

This is the most fragile ingestion source in the project: it parses a Wikipedia
page whose column headers change wording periodically and whose tables move
between articles. Getting the wrong table would silently produce a membership
timeline with no history, which reintroduces exactly the survivorship bias the
pipeline exists to remove.

That is not hypothetical. The scraper originally took the *second* wikitable on
"List of S&P 500 companies"; by 2026-08 that table had been moved into its own
article, leaving one wikitable behind and breaking the scrape. The client now
finds the table by its "Added"/"Removed" headers, and the first test below
pins that behaviour by putting the changes table first, last, and alone.

The HTML fixtures are trimmed versions of the real page, including the
"Effective Date / Added / Removed" multi-level header that
`_normalise_changes` exists to flatten.
"""

from __future__ import annotations

import pandas as pd
import pytest
import responses

from tierzero.ingestion.sp500_membership import _CHANGES_URL, SP500MembershipScraper


CONSTITUENTS_TABLE = """
<table class="wikitable" id="constituents">
  <tr><th>Symbol</th><th>Security</th></tr>
  <tr><td>AAPL</td><td>Apple Inc.</td></tr>
  <tr><td>MSFT</td><td>Microsoft</td></tr>
</table>
"""

CHANGES_TABLE = """
<table class="wikitable" id="changes">
  <tr>
    <th rowspan="2">Effective Date</th>
    <th colspan="2">Added</th>
    <th colspan="2">Removed</th>
  </tr>
  <tr>
    <th>Ticker</th><th>Security</th><th>Ticker</th><th>Security</th>
  </tr>
  <tr>
    <td>June 20, 2024</td>
    <td>CRWD</td><td>CrowdStrike</td>
    <td>CMA</td><td>Comerica</td>
  </tr>
  <tr>
    <td>October 1, 2023</td>
    <td>LULU</td><td>Lululemon</td>
    <td>ATVI</td><td>Activision Blizzard</td>
  </tr>
</table>
"""


def _page(*tables: str) -> str:
    return f"<html><body>{''.join(tables)}</body></html>"


@pytest.fixture
def scraper(tmp_path) -> SP500MembershipScraper:
    return SP500MembershipScraper(output_dir=tmp_path)


class TestScrapeWikipedia:

    @pytest.mark.parametrize(
        "layout",
        [
            pytest.param((CHANGES_TABLE,), id="alone"),
            pytest.param((CONSTITUENTS_TABLE, CHANGES_TABLE), id="second"),
            pytest.param((CHANGES_TABLE, CONSTITUENTS_TABLE), id="first"),
        ],
    )
    @responses.activate
    def test_changes_table_is_found_regardless_of_position(self, scraper, layout):
        """
        The regression guard. Position on the page is not a stable identifier —
        it has already changed once. Picking the constituent table instead would
        yield a universe with no add/remove history, i.e. survivorship bias.
        """
        responses.add(responses.GET, _CHANGES_URL, body=_page(*layout), status=200)

        df = scraper.scrape_wikipedia()

        assert "ticker_added" in df.columns, "Parsed the constituent table by mistake"
        assert set(df["ticker_added"]) == {"CRWD", "LULU"}
        assert set(df["ticker_removed"]) == {"CMA", "ATVI"}

    @responses.activate
    def test_multilevel_headers_are_flattened_to_canonical_names(self, scraper):
        responses.add(
            responses.GET, _CHANGES_URL,
            body=_page(CONSTITUENTS_TABLE, CHANGES_TABLE), status=200,
        )

        df = scraper.scrape_wikipedia()

        assert set(df.columns) == {
            "date", "ticker_added", "name_added", "ticker_removed", "name_removed"
        }

    @responses.activate
    def test_dates_are_parsed_to_timestamps(self, scraper):
        responses.add(
            responses.GET, _CHANGES_URL,
            body=_page(CONSTITUENTS_TABLE, CHANGES_TABLE), status=200,
        )

        df = scraper.scrape_wikipedia()

        assert pd.api.types.is_datetime64_any_dtype(df["date"])
        assert pd.Timestamp("2024-06-20") in set(df["date"])

    @responses.activate
    def test_missing_changes_table_raises(self, scraper):
        """
        If Wikipedia restructures the page, failing loudly beats writing an
        empty timeline that later looks like "the index never changed".
        """
        responses.add(
            responses.GET, _CHANGES_URL, body=_page(CONSTITUENTS_TABLE), status=200
        )

        with pytest.raises(RuntimeError, match="page structure changed"):
            scraper.scrape_wikipedia()

    @responses.activate
    def test_http_error_propagates(self, scraper):
        responses.add(responses.GET, _CHANGES_URL, status=503)

        with pytest.raises(Exception):
            scraper.scrape_wikipedia()

    @responses.activate
    def test_result_is_cached_to_parquet(self, scraper, tmp_path):
        responses.add(
            responses.GET, _CHANGES_URL,
            body=_page(CONSTITUENTS_TABLE, CHANGES_TABLE), status=200,
        )

        scraper.scrape_wikipedia()

        assert (tmp_path / "wikipedia_changes.parquet").exists()

    @responses.activate
    def test_second_call_uses_the_cache(self, scraper):
        responses.add(
            responses.GET, _CHANGES_URL,
            body=_page(CONSTITUENTS_TABLE, CHANGES_TABLE), status=200,
        )

        first = scraper.scrape_wikipedia()
        second = scraper.scrape_wikipedia()

        assert len(responses.calls) == 1, "Cached call should not re-request"
        pd.testing.assert_frame_equal(first, second)

    @responses.activate
    def test_force_bypasses_the_cache(self, scraper):
        responses.add(
            responses.GET, _CHANGES_URL,
            body=_page(CONSTITUENTS_TABLE, CHANGES_TABLE), status=200,
        )

        scraper.scrape_wikipedia()
        scraper.scrape_wikipedia(force=True)

        assert len(responses.calls) == 2


class TestNormaliseChanges:

    def test_rows_without_a_usable_date_are_dropped(self):
        """
        The real table carries footnote and "—" rows that parse to NaT. They
        must not reach the timeline builder as undated events.
        """
        raw = pd.DataFrame({
            "Date": ["June 20, 2024", "not a date", None],
            "Added Ticker": ["CRWD", "XXX", "YYY"],
            "Removed Ticker": ["CMA", "ZZZ", "WWW"],
        })

        out = SP500MembershipScraper._normalise_changes(raw)

        assert len(out) == 1
        assert out.loc[0, "ticker_added"] == "CRWD"

    def test_ticker_whitespace_is_stripped(self):
        raw = pd.DataFrame({
            "Date": ["June 20, 2024"],
            "Added Ticker": ["  CRWD "],
            "Removed Ticker": ["CMA  "],
        })

        out = SP500MembershipScraper._normalise_changes(raw)

        assert out.loc[0, "ticker_added"] == "CRWD"
        assert out.loc[0, "ticker_removed"] == "CMA"
