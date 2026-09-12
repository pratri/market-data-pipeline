"""
Builds data/ticker_cik_map.json (ticker -> zero-padded CIK).

The companyfacts API takes CIKs, not tickers. Source is SEC's public
https://www.sec.gov/files/company_tickers.json. SEC wants a name and email
in the User-Agent, read from SEC_USER_AGENT in .env.

Only the current CIK is stored. Tickers with a predecessor registrant are
handled in ingest_fundamentals.PREDECESSOR_CIKS.
"""

import json
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent))
from ingest_fundamentals import get_user_agent

SEC_TICKER_URL = "https://www.sec.gov/files/company_tickers.json"

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
    """Fetch SEC's ticker file and return {TICKER: {cik, title}}.

    SEC's JSON looks like {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}, ...}
    """
    response = requests.get(
        SEC_TICKER_URL,
        headers={"User-Agent": get_user_agent()},
        timeout=15,
    )
    response.raise_for_status()

    lookup = {}
    for entry in response.json().values():
        ticker = entry["ticker"].upper()
        # API wants CIK0000320193, the JSON gives 320193
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
