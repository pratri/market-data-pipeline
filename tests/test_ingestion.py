"""Unit tests for the SEC extraction rules.

The DuckDB run starts from fixtures that ingestion already produced, so it
can't see any of this. Each case below is a bug that reached the warehouse
once.

    python tests/test_ingestion.py
"""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "scripts"))

import pandas as pd

import ingest_fundamentals as ing

failures: list = []


def fact(val, start, end, form, filed, accn):
    return {"start": start, "end": end, "val": val, "form": form, "filed": filed,
            "accn": accn, "fy": 2025, "fp": "Q2"}


def facts(**tags):
    """companyfacts-shaped dict: {tag: [entries]} under us-gaap."""
    return {"facts": {"us-gaap": {t: {"units": {"USD": e}} for t, e in tags.items()}}}


def expect(name, got, want):
    if got != want:
        failures.append(f"{name}: got {got!r}, expected {want!r}")


def test_first_filing_wins():
    """A period repeated as a comparative keeps its original filing date.

    Keeping the newest copy pushed filed dates forward by up to two years,
    and the as-of join then served year-old quarters.
    """
    rows = ing.extract_metric(facts(Revenues=[
        fact(100, "2025-01-01", "2025-03-31", "10-Q", "2025-04-30", "a"),
        fact(100, "2025-01-01", "2025-03-31", "10-Q", "2026-04-30", "b"),   # comparative
    ]), ["Revenues"])
    df = ing.dedupe_restatements(pd.DataFrame(rows).assign(ticker="T", metric="revenue", _cik_rank=0))
    expect("first filing kept", len(df), 1)
    expect("filed date is the original", str(df.filed.iloc[0].date()), "2025-04-30")


def test_only_periodic_reports():
    """8-K recasts and proxy statements repeat old numbers on a new basis."""
    rows = ing.extract_metric(facts(Revenues=[
        fact(100, "2025-01-01", "2025-03-31", "10-Q", "2025-04-30", "a"),
        fact(999, "2025-04-01", "2025-06-30", "8-K", "2025-08-01", "b"),
        fact(999, "2024-01-01", "2024-12-31", "DEF 14A", "2025-04-01", "c"),
    ]), ["Revenues"])
    expect("only the 10-Q survives", [r["form"] for r in rows], ["10-Q"])


def test_amendment_loses_a_same_day_tie():
    # amendment listed first, so only the tie-break can put it second
    rows = ing.extract_metric(facts(Revenues=[
        fact(111, "2025-01-01", "2025-03-31", "10-Q/A", "2025-04-30", "b"),
        fact(100, "2025-01-01", "2025-03-31", "10-Q", "2025-04-30", "a"),
    ]), ["Revenues"])
    df = ing.dedupe_restatements(pd.DataFrame(rows).assign(ticker="T", metric="revenue", _cik_rank=0))
    expect("original beats amendment on the same day", df.value.iloc[0], 100)


def test_largest_revenue_tag_wins():
    """Revenue tags overlap in both directions, so size decides, not order.

    COP's Revenues is the income statement total and its
    RevenueFromContractWithCustomer is part of it. BLK's FY2024 10-K is the
    other way round. A total can't be smaller than one of its parts.
    """
    both = {"facts": {"us-gaap": {
        "Revenues": {"units": {"USD": [fact(13.3, "2025-01-01", "2025-03-31", "10-Q", "2025-04-30", "a")]}},
        "RevenueFromContractWithCustomerExcludingAssessedTax": {
            "units": {"USD": [fact(15.0, "2025-01-01", "2025-03-31", "10-Q", "2025-04-30", "a")]}},
    }}}
    rows = ing.extract_metric(both, ing.METRIC_TAGS["revenue"], pick_largest=True)
    expect("largest revenue tag wins", [r["value"] for r in rows], [15.0])

    rows = ing.extract_metric(both, ing.METRIC_TAGS["revenue"], pick_largest=False)
    expect("without the rule, list order wins", [r["value"] for r in rows], [13.3])


def test_revenue_is_wired_to_the_largest_rule():
    """process_company has to pass pick_largest for revenue and not for the rest."""
    fx = {"facts": {"us-gaap": {
        "Revenues": {"units": {"USD": [fact(13.3, "2025-01-01", "2025-03-31", "10-Q", "2025-04-30", "a")]}},
        "RevenueFromContractWithCustomerExcludingAssessedTax": {
            "units": {"USD": [fact(15.0, "2025-01-01", "2025-03-31", "10-Q", "2025-04-30", "a")]}},
        "NetIncomeLoss": {"units": {"USD": [fact(90, "2025-01-01", "2025-03-31", "10-Q", "2025-04-30", "a")]}},
        "ProfitLoss": {"units": {"USD": [fact(95, "2025-01-01", "2025-03-31", "10-Q", "2025-04-30", "a")]}},
    }}}
    df = ing.process_company("T", [("0000000001", fx)])
    expect("revenue takes the larger tag", df.loc[df.metric == "revenue", "value"].tolist(), [15.0])
    expect("net income keeps priority order", df.loc[df.metric == "net_income", "value"].tolist(), [90])


def test_net_income_uses_tag_priority():
    """NetIncomeLoss is the parent's share, ProfitLoss includes minorities."""
    both = {"facts": {"us-gaap": {
        "NetIncomeLoss": {"units": {"USD": [fact(90, "2025-01-01", "2025-03-31", "10-Q", "2025-04-30", "a")]}},
        "ProfitLoss": {"units": {"USD": [fact(95, "2025-01-01", "2025-03-31", "10-Q", "2025-04-30", "a")]}},
    }}}
    rows = ing.extract_metric(both, ing.METRIC_TAGS["net_income"],
                              pick_largest="net_income" in ing.LARGEST_WINS)
    expect("net income keeps priority order", [r["value"] for r in rows], [90])


def test_tags_are_merged_not_first_match():
    """NVDA switched revenue tags in 2022, and stopping at the first lost four years."""
    rows = ing.extract_metric(facts(
        RevenueFromContractWithCustomerExcludingAssessedTax=[
            fact(10, "2021-01-01", "2021-03-31", "10-Q", "2021-04-30", "a")],
        Revenues=[fact(20, "2023-01-01", "2023-03-31", "10-Q", "2023-04-30", "b")],
    ), ing.METRIC_TAGS["revenue"], pick_largest=True)
    expect("both tags come through", sorted(r["value"] for r in rows), [10, 20])


def test_predecessor_ciks_are_configured():
    """XOM and BLK file under a new registrant after reorganising."""
    expect("XOM has a predecessor", ing.PREDECESSOR_CIKS.get("XOM"), ["0000034088"])
    expect("BLK has a predecessor", ing.PREDECESSOR_CIKS.get("BLK"), ["0001364742"])


def test_current_cik_wins_a_tie():
    """Both registrants can file the same period on the same day."""
    # predecessor listed first, so only the tie-break can put it second
    rows = [
        dict(period_start="2025-01-01", period_end="2025-03-31", value=2, filed="2025-04-30",
             form="10-Q", ticker="XOM", metric="revenue", cik="old", _cik_rank=1),
        dict(period_start="2025-01-01", period_end="2025-03-31", value=1, filed="2025-04-30",
             form="10-Q", ticker="XOM", metric="revenue", cik="new", _cik_rank=0),
    ]
    df = ing.dedupe_restatements(pd.DataFrame(rows))
    expect("current CIK wins", df.cik.iloc[0], "new")


def test_cover_shares_come_from_dei():
    fx = {"facts": {"dei": {"EntityCommonStockSharesOutstanding": {"units": {"shares": [
        fact(710_398_642, None, "2026-01-31", "10-K", "2026-02-24", "a")]}}}}}
    rows = ing.extract_metric(fx, ing.DEI_METRIC_TAGS["cover_shares_outstanding"], namespace="dei")
    expect("cover count read from dei", [r["value"] for r in rows], [710_398_642])


def main():
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            try:
                fn()
            except Exception as exc:
                failures.append(f"{name} raised {type(exc).__name__}: {exc}")
    for f in failures:
        print(f"FAIL  {f}")
    print(f"\n{len(failures)} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
