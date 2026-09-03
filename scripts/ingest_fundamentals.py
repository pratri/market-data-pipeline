"""
Pulls quarterly fundamentals from SEC EDGAR and lands them as Parquet in S3.

Three real problems this handles:

1. TAG INCONSISTENCY. Companies report the same concept under different
   US-GAAP tags. Revenue might be Revenues,
   RevenueFromContractWithCustomerExcludingAssessedTax, or SalesRevenueNet.
   A priority list per metric resolves it; first tag present wins.

2. RESTATEMENTS. The same fiscal period appears repeatedly as later
   filings restate it. Each entry has a `filed` date, so we keep the
   most recently filed value per period.

3. RATE LIMITING. SEC allows ~10 req/sec. One sequential call per
   company with a deliberate delay stays well under.

Fundamentals update quarterly, so unlike prices this writes a single
snapshot object rather than date partitions.

Usage:
    python scripts/ingest_fundamentals.py
    python scripts/ingest_fundamentals.py --limit 5
"""

import argparse
import json
import os
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).parent))
from s3_utils import get_bucket, load_env, write_parquet_to_s3

PROJECT_ROOT = Path(__file__).parent.parent
TICKER_MAP_PATH = PROJECT_ROOT / "data" / "ticker_cik_map.json"

S3_PREFIX = "raw/fundamentals"
FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

REQUEST_DELAY = 0.15     # ~6.7 req/sec, under SEC's 10/sec limit
MAX_RETRIES = 3
INITIAL_BACKOFF = 3

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
    "operating_income": ["OperatingIncomeLoss"],
    "total_assets": ["Assets"],
    "total_liabilities": ["Liabilities"],
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


def get_user_agent() -> str:
    """SEC blocks requests that don't identify the caller by name and email.

    Read from .env rather than hardcoded so a personal address doesn't
    end up in a public repo.
    """
    load_env()
    ua = os.environ.get("SEC_USER_AGENT")
    if not ua or "example.com" in ua:
        raise SystemExit(
            "ERROR: SEC_USER_AGENT is not set.\n"
            "Add this to your .env file:\n"
            '  SEC_USER_AGENT=Pranav your-real-email@domain.com\n'
            "The SEC blocks requests that don't identify the caller."
        )
    return ua


def load_ticker_map() -> dict:
    if not TICKER_MAP_PATH.exists():
        sys.exit(
            f"ERROR: {TICKER_MAP_PATH} not found.\n"
            "Run scripts/build_ticker_universe.py first."
        )
    return json.loads(TICKER_MAP_PATH.read_text())


def fetch_company_facts(session: requests.Session, cik: str, ua: str) -> dict | None:
    """Fetch one company's facts with retry. None on hard failure."""
    url = FACTS_URL.format(cik=cik)
    backoff = INITIAL_BACKOFF

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, headers={"User-Agent": ua}, timeout=20)

            if resp.status_code == 404:
                return None          # genuinely no XBRL facts; not retryable
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

    Structure: facts["facts"]["us-gaap"][TAG]["units"][UNIT] -> list of
    entries with start/end dates, val, fy, fp, form, filed.
    """
    us_gaap = facts.get("facts", {}).get("us-gaap", {})

    for tag in tag_candidates:
        if tag not in us_gaap:
            continue

        units = us_gaap[tag].get("units", {})
        unit_key = next(
            (u for u in ("USD", "shares", "USD/shares") if u in units),
            None,
        )
        if unit_key is None:
            continue

        rows = []
        for entry in units[unit_key]:
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
    """Keep the most recently filed value per (ticker, metric, period).

    period_start is part of the key deliberately: a Q2 quarterly figure
    and a half-year figure share an end date but are different facts.
    Collapsing them would silently drop real data.
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
    parser = argparse.ArgumentParser(description="Ingest SEC fundamentals to S3.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only process the first N tickers (smoke test)")
    args = parser.parse_args()

    ua = get_user_agent()
    bucket = get_bucket()

    ticker_map = load_ticker_map()
    items = sorted(ticker_map.items())
    if args.limit:
        items = items[:args.limit]

    print(f"Bucket:    s3://{bucket}/{S3_PREFIX}")
    print(f"Companies: {len(items)} (~{len(items) * REQUEST_DELAY:.0f}s minimum)\n")

    session = requests.Session()
    frames = []
    failed = []

    for i, (ticker, info) in enumerate(items, 1):
        cik = info["cik"]
        print(f"[{i}/{len(items)}] {ticker} (CIK {cik})")

        facts = fetch_company_facts(session, cik, ua)
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

    # Snapshot key, dated so history is preserved rather than overwritten.
    # Fundamentals change slowly; a full replace each run is simpler than
    # incremental logic and the volume is small enough not to matter.
    key = f"{S3_PREFIX}/snapshot_date={date.today()}/fundamentals.parquet"
    uri = write_parquet_to_s3(combined, key, bucket=bucket)

    print(f"\nWrote {len(combined):,} rows to {uri}")
    print(f"Companies with data: {combined['ticker'].nunique()} / {len(items)}")
    print(f"Metrics covered: {sorted(combined['metric'].unique())}")
    print(f"Period range: {combined['period_end'].min()} to {combined['period_end'].max()}")

    if failed:
        print(f"\nWARNING: {len(failed)} companies returned nothing: {failed}")


if __name__ == "__main__":
    main()
