"""
Pulls fundamentals from SEC EDGAR companyfacts and writes a dated parquet
snapshot to S3.

- A metric can show up under several us-gaap tags, so all candidate tags
  are read and merged.
- Every 10-Q/10-K repeats earlier periods as comparatives. Only the first
  filing of each period is kept, so `filed` is when the number became
  public and the value is what was originally reported.
- Only 10-K/10-Q filings and their amendments are used. 8-K recasts and
  proxy statements repeat old numbers months or years later.
- SEC allows about 10 requests/sec, so calls are spaced out.

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

REQUEST_DELAY = 0.15     # ~6.7 req/sec
MAX_RETRIES = 3
INITIAL_BACKOFF = 3

# Originals before amendments when two filings land on the same day.
FORM_RANK = {
    "10-K": 0, "10-Q": 0, "10-KT": 0, "10-QT": 0,
    "10-K/A": 1, "10-Q/A": 1, "10-KT/A": 1, "10-QT/A": 1,
}

# Tickers whose history is split across two SEC registrants after a holding
# company reorganization. The ticker map only has the current CIK.
PREDECESSOR_CIKS = {
    "XOM": ["0000034088"],   # Exxon Mobil Corp, still files alongside the 2026 holdco
    "BLK": ["0001364742"],   # BlackRock Finance, everything before Nov 2024
}

# Metrics where, if one filing tags the same period under several candidate
# tags, the largest value wins instead of the first listed. Revenue tags
# overlap in both directions: COP's Revenues (15.0bn) is its income statement
# total and RevenueFromContractWithCustomer (13.3bn) is the ASC 606 part, but
# BLK's FY2024 10-K has Revenues at 12.8bn under a RevenueFromContract total
# of 20.4bn. A total can't be smaller than a piece of it.
LARGEST_WINS = {"revenue"}

METRIC_TAGS = {
    "revenue": [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
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

# Cover page share count. A real count dated a few weeks after quarter end,
# on nearly every 10-Q/10-K. Multi-class companies only report it per class,
# which companyfacts leaves out.
DEI_METRIC_TAGS = {
    "cover_shares_outstanding": ["EntityCommonStockSharesOutstanding"],
}


def get_user_agent() -> str:
    """SEC blocks requests without a name and email in the User-Agent.

    Read from .env so the address stays out of the repo.
    """
    load_env()
    ua = os.environ.get("SEC_USER_AGENT")
    if not ua or "example.com" in ua or "your-email" in ua:
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
    """Fetch one company's facts with retries. None if it fails."""
    url = FACTS_URL.format(cik=cik)
    backoff = INITIAL_BACKOFF

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, headers={"User-Agent": ua}, timeout=20)

            if resp.status_code == 404:
                return None          # no XBRL facts, don't retry
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


def extract_metric(
    facts: dict,
    tag_candidates: list[str],
    namespace: str = "us-gaap",
    pick_largest: bool = False,
) -> list[dict]:
    """Collect a metric's values across all candidate tags.

    facts["facts"][namespace][TAG]["units"][UNIT] is a list of entries with
    start/end, val, fy, fp, form and filed.

    Companies switch tags over time. NVDA used
    RevenueFromContractWithCustomerExcludingAssessedTax until Jan 2022 and
    Revenues after, so stopping at the first tag with data lost four years
    of NVDA revenue. When two tags report the same period in the same
    filing, the one listed first wins, or the larger one if pick_largest.
    """
    tags_in_namespace = facts.get("facts", {}).get(namespace, {})

    # same fact under two tags collapses to one row, different periods don't
    merged: dict[tuple, dict] = {}

    for priority, tag in enumerate(tag_candidates):
        if tag not in tags_in_namespace:
            continue

        units = tags_in_namespace[tag].get("units", {})
        unit_key = next(
            (u for u in ("USD", "shares", "USD/shares") if u in units),
            None,
        )
        if unit_key is None:
            continue

        for entry in units[unit_key]:
            if "end" not in entry or "val" not in entry:
                continue
            if entry.get("form") not in FORM_RANK:
                continue

            key = (
                entry.get("start"),
                entry["end"],
                entry.get("form"),
                entry.get("accn"),
            )

            existing = merged.get(key)
            if existing is not None:
                if pick_largest:
                    if abs(entry["val"]) <= abs(existing["value"]):
                        continue
                # lower number = earlier in the list = wins
                elif existing["_priority"] <= priority:
                    continue

            merged[key] = {
                "_priority": priority,
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
            }

    # drop the helper field
    return [
        {k: v for k, v in row.items() if k != "_priority"}
        for row in merged.values()
    ]


def dedupe_restatements(df: pd.DataFrame) -> pd.DataFrame:
    """Keep the first filing of each (ticker, metric, period_start, period_end).

    Keeping the latest copy instead moved `filed` forward to whichever later
    filing last repeated the number (up to two years), and the as-of join in
    dbt then attached year-old quarters. Same-day ties go to originals over
    amendments, then to the current CIK over a predecessor.
    """
    if df.empty:
        return df

    df = df.copy()
    df["filed"] = pd.to_datetime(df["filed"], errors="coerce")
    df = df.dropna(subset=["filed"])
    df["_form_rank"] = df["form"].map(FORM_RANK)
    df = df.sort_values(["filed", "_form_rank", "_cik_rank"], kind="stable")

    return (
        df.drop_duplicates(
            subset=["ticker", "metric", "period_start", "period_end"],
            keep="first",
        )
        .drop(columns=["_form_rank", "_cik_rank"])
        .reset_index(drop=True)
    )


def process_company(ticker: str, facts_by_cik: list[tuple[str, dict]]) -> pd.DataFrame:
    """Rows for one ticker. facts_by_cik is [(cik, facts)], current CIK first."""
    rows = []
    for cik_rank, (cik, facts) in enumerate(facts_by_cik):
        for namespace, metric_tags in (("us-gaap", METRIC_TAGS), ("dei", DEI_METRIC_TAGS)):
            for metric, tags in metric_tags.items():
                for row in extract_metric(facts, tags, namespace, metric in LARGEST_WINS):
                    row["ticker"] = ticker
                    row["cik"] = cik
                    row["metric"] = metric
                    row["_cik_rank"] = cik_rank
                    rows.append(row)

    if not rows:
        return pd.DataFrame()

    return dedupe_restatements(pd.DataFrame(rows))


def fetch_ticker(session: requests.Session, ticker: str, cik: str, ua: str) -> pd.DataFrame:
    """Fetch and process one ticker, including any predecessor CIKs."""
    facts_by_cik = []
    for c in [cik, *PREDECESSOR_CIKS.get(ticker, [])]:
        facts = fetch_company_facts(session, c, ua)
        time.sleep(REQUEST_DELAY)
        if facts is None:
            print(f"    no facts returned for CIK {c}")
            continue
        facts_by_cik.append((c, facts))

    return process_company(ticker, facts_by_cik)


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

        df = fetch_ticker(session, ticker, cik, ua)
        if df.empty:
            print("    no matching metrics found")
            failed.append(ticker)
        else:
            print(f"    {len(df):,} rows across {df['metric'].nunique()} metrics")
            frames.append(df)

    if not frames:
        sys.exit("\nERROR: no fundamentals retrieved.")

    combined = pd.concat(frames, ignore_index=True)

    # Dated key so older snapshots aren't overwritten. dbt reads each
    # ticker's latest snapshot, so a ticker that failed today keeps its
    # previous data instead of disappearing.
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
