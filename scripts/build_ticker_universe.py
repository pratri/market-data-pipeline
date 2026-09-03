"""
Maps ticker symbols to SEC CIK numbers.

The SEC's company facts API doesn't accept ticker symbols. To pull
Apple's financials you need its CIK (Central Index Key), zero-padded
to 10 digits: CIK0000320193. This script builds that mapping once so
the fundamentals ingestion script can look it up.

Source: https://www.sec.gov/files/company_tickers.json (public, no auth)

Set USER_AGENT below to your real name and email before running.
The SEC blocks requests without a descriptive User-Agent.
"""

import json
import sys
from pathlib import Path

import requests

SEC_TICKER_URL = "https://www.sec.gov/files/company_tickers.json"

# REQUIRED: replace with your real name and email.
USER_AGENT = "Pranav pranteja@gmail.com"

OUTPUT_PATH = Path(__file__).parent.parent / "data" / "ticker_cik_map.json"

TICKER_UNIVERSE = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
    "JPM", "V", "MA", "UNH", "HD", "PG", "JNJ", "MRK", "ABBV", "AVGO",
    "COST", "PEP", "KO", "WMT", "MCD", "DIS", "NFLX", "ADBE", "CRM",
    "ORCL", "CSCO", "INTC", "AMD", "QCOM", "TXN", "IBM", "NOW", "INTU",
    "XOM", "CVX", "COP", "SLB", "CAT", "DE", "BA", "GE", "HON", "UPS",
    "LMT", "RTX", "GS", "MS", "BAC", "WFC", "C", "AXP", "BLK", "SPGI",
    "SCHW", "PFE", "ABT", "TMO", "DHR", "BMY", "AMGN", "GILD", "CVS",
]


def fetch_sec_lookup() -> dict:
    """Fetch and parse the SEC's full ticker-to-CIK file.

    SEC returns a dict with meaningless numeric string keys:
        {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}, ...}

    Returns a dict keyed by uppercase ticker with zero-padded CIKs.
    """
    if "your-email@example.com" in USER_AGENT:
        sys.exit(
            "ERROR: Set USER_AGENT to your real name and email before running.\n"
            "The SEC blocks requests that don't identify the caller."
        )

    response = requests.get(
        SEC_TICKER_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=15,
    )
    response.raise_for_status()

    lookup = {}
    for entry in response.json().values():
        ticker = entry["ticker"].upper()
        # zfill(10) because the API endpoint requires CIK0000320193,
        # not CIK320193. The JSON gives us the unpadded integer.
        lookup[ticker] = {
            "cik": str(entry["cik_str"]).zfill(10),
            "title": entry["title"],
        }
    return lookup


def build_mapping() -> dict:
    sec_lookup = fetch_sec_lookup()
    print(f"Fetched {len(sec_lookup):,} ticker mappings from SEC.")

    mapping = {}
    missing = []
    for ticker in TICKER_UNIVERSE:
        if ticker in sec_lookup:
            mapping[ticker] = sec_lookup[ticker]
        else:
            missing.append(ticker)

    if missing:
        print(f"\nWARNING: {len(missing)} ticker(s) not found: {missing}")
        print("Share class tickers often differ in SEC's file "
              "(BRK.B may be listed as BRK-B or BRKB). Check these manually.")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(mapping, indent=2))
    print(f"\nWrote {len(mapping)} mappings to {OUTPUT_PATH}")
    return mapping


if __name__ == "__main__":
    build_mapping()