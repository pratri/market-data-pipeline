"""
Pulls quarterly fundamentals from SEC EDGAR's company facts API and
lands them as Parquet.

Three real problems this handles:

1. TAG INCONSISTENCY. Companies report the same concept under
   different US-GAAP tags depending on filer and year. Revenue might
   be Revenues, RevenueFromContractWithCustomerExcludingAssessedTax,
   or SalesRevenueNet. We define a priority list per metric and take
   the first tag present for each company.

2. RESTATEMENTS. The same fiscal period appears multiple times: once
   from the original 10-Q, again from later filings that restate it.
   Each entry carries a `filed` date, so we keep the most recently
   filed value per (metric, period).

3. RATE LIMITING. SEC allows ~10 req/sec and blocks abusers. We make
   one call per company, sequentially, with a deliberate delay.

Usage:
    python scripts/ingest_fundamentals.py
    python scripts/ingest_fundamentals.py --limit 5    # smoke test
"""

import argparse
import json
import sys
import time
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).parent.parent
TICKER_MAP_PATH = PROJECT_ROOT / "data" / "ticker_cik_map.json"
OUTPUT_DIR = PROJECT_ROOT / "data" / "fundamentals"

FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

# REQUIRED: replace with your real name and email, same as the
# ticker script. SEC blocks requests that don't identify the caller.
USER_AGENT = "Pranav pranteja.com>"

REQUEST_DELAY = 0.15     # ~6.7 req/sec, comfortably under SEC's 10/sec
MAX_RETRIES = 3
INITIAL_BACKOFF = 3

# Ordered by preference. First tag found for a company wins.
METRIC_TAGS = {
    "revenue": [
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "Revenues",
        "SalesRevenueNet",
        "SalesRevenueGoodsNet",
    ],
    "net_income": [
        "NetIncomeLoss",
        "ProfitLoss",
        "NetIncomeLossAvailableToCommonStockholdersBasic",
    ],
    "operating_income": [
        "OperatingIncomeLoss",
    ],
    "total_assets": [
        "Assets",
    ],
    "total_liabilities": [
        "Liabilities",
    ],
    "stockholders_equity": [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ],
    "shares_outstanding": [
        "CommonStockSharesOutstanding",
        "WeightedAverageNumberOfDilutedSharesOutstanding",
        "WeightedAverageNumberOfSharesOutstandingBasic",
    ],
    "cash_and_equivalents": [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    ],
    "operating_cash_flow": [
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
    ],
}


def load_ticker_map() -> dict:
    if not TICKER_MAP_PATH.exists():
        sys.exit(
            f"ERROR: {TICKER_MAP_PATH} not found.\n"
            "Run scripts/build_ticker_universe.py first."
        )
    return json.loads(TICKER_MAP_PATH.read_text())


def fetch_company_facts(session: requests.Session, cik: str) -> dict | None:
    """Fetch one company's facts with retry. Returns None on hard failure."""
    url = FACTS_URL.format(cik=cik)
    backoff = INITIAL_BACKOFF

    for attempt in range(1, MAX_RETRIES + 1):
        try:  
            resp = session.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)

            if resp.status_code == 404:
                # Some CIKs genuinely have no XBRL facts. Not retryable.
                return None
            if resp.status_code == 429:
                print(f"    rate limited, backing off {backoff}s")
                time.sleep(backoff)
                backoff *= 2
                continue

            resp.raise_for_status()
            return resp.json()

        except Exception as exc:
            print(f"    attempt {attempt} failed ({type(exc).__name__}: {exc})")
            if attempt < MAX_RETRIES:
                time.sleep(backoff)
                backoff *= 2

    return None


def extract_metric(facts: dict, tag_candidates: list[str]) -> list[dict]:
    """Pull all reported values for the first matching tag.

    Structure is facts["facts"]["us-gaap"][TAG]["units"][UNIT] -> list
    of entries. Each entry has start/end dates, val, fy, fp, form,
    filed, and sometimes frame.
    """
    us_gaap = facts.get("facts", {}).get("us-gaap", {})

    for tag in tag_candidates:
        if tag not in us_gaap:
            continue

        units = us_gaap[tag].get("units", {})
        # Prefer USD; fall back to share counts for share metrics.
        unit_key = next(
            (u for u in ("USD", "shares", "USD/shares") if u in units),
            None,
        )
        if unit_key is None:
            continue

        rows = []
        for entry in units[unit_key]:
            # Only periodic reports; skip anything without an end date.
            if "end" not in entry or "val" not in entry:
                continue
            rows.append({
                "tag": tag,
                "unit": unit_key,
                "period_start": entry.get("start"),
                "period_end": entry["end"],
                "value": entry["val"],
                "fiscal_year": entry.get("fy"),
                "fiscal_period": entry.get("fp"),
                "form": entry.get("form"),
                "filed": entry.get("filed"),
                "accession": entry.get("accn"),
            })
        if rows:
            return rows

    return []


def dedupe_restatements(df: pd.DataFrame) -> pd.DataFrame:
    """Keep the most recently filed value per (metric, period, form type).

    The same fiscal period is reported repeatedly as later filings
    restate it. Sorting by filed date and keeping the last gives the
    company's most current view of that period.
    """
    if df.empty:
        return df

    df = df.copy()
    df["filed"] = pd.to_datetime(df["filed"], errors="coerce")
    df = df.sort_values("filed")

    return (
        df.drop_duplicates(
            subset=["ticker", "metric", "period_start", "period_end"],
            keep="last",
        )
        .reset_index(drop=True)
    )


def process_company(ticker: str, cik: str, facts: dict) -> pd.DataFrame:
    rows = []
    for metric, tags in METRIC_TAGS.items():
        for row in extract_metric(facts, tags):
            row["ticker"] = ticker
            row["cik"] = cik
            row["metric"] = metric
            rows.append(row)

    if not rows:
        return pd.DataFrame()

    return dedupe_restatements(pd.DataFrame(rows))


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest SEC fundamentals.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only process the first N tickers (smoke test)")
    args = parser.parse_args()

    if "your-email@example.com" in USER_AGENT:
        sys.exit(
            "ERROR: Set USER_AGENT to your real name and email before running.\n"
            "The SEC blocks requests that don't identify the caller."
        )

    ticker_map = load_ticker_map()
    items = sorted(ticker_map.items())
    if args.limit:
        items = items[:args.limit]

    print(f"Fetching fundamentals for {len(items)} companies "
          f"(~{len(items) * REQUEST_DELAY:.0f}s minimum)\n")

    session = requests.Session()
    frames = []
    failed = []

    for i, (ticker, info) in enumerate(items, 1):
        cik = info["cik"]
        print(f"[{i}/{len(items)}] {ticker} (CIK {cik})")

        facts = fetch_company_facts(session, cik)
        if facts is None:
            print("    no facts returned")
            failed.append(ticker)
            time.sleep(REQUEST_DELAY)
            continue

        df = process_company(ticker, cik, facts)
        if df.empty:
            print("    no matching metrics found")
            failed.append(ticker)
        else:
            print(f"    {len(df):,} rows across {df['metric'].nunique()} metrics")
            frames.append(df)

        time.sleep(REQUEST_DELAY)

    if not frames:
        sys.exit("\nERROR: no fundamentals retrieved.")

    combined = pd.concat(frames, ignore_index=True)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "fundamentals.parquet"
    combined.to_parquet(out_path, index=False, compression="snappy")

    print(f"\nWrote {len(combined):,} rows to {out_path}")
    print(f"Companies with data: {combined['ticker'].nunique()} / {len(items)}")
    print(f"Metrics covered: {sorted(combined['metric'].unique())}")
    print(f"Period range: {combined['period_end'].min()} to {combined['period_end'].max()}")

    if failed:
        print(f"\nWARNING: {len(failed)} companies returned nothing: {failed}")


if __name__ == "__main__":
    main()