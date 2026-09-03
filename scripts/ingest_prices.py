"""
Pulls daily OHLCV price data and lands it as date-partitioned Parquet.

Reads the ticker list from data/ticker_cik_map.json so there's one
source of truth for the universe.

yfinance scrapes Yahoo rather than using a licensed API, so it rate
limits and fails intermittently. Retry with exponential backoff is
required, not optional.

Usage:
    python scripts/ingest_prices.py
    python scripts/ingest_prices.py --start 2024-01-01 --end 2024-06-30
"""

import argparse
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

PROJECT_ROOT = Path(__file__).parent.parent
TICKER_MAP_PATH = PROJECT_ROOT / "data" / "ticker_cik_map.json"
OUTPUT_DIR = PROJECT_ROOT / "data" / "prices"

BATCH_SIZE = 20          # tickers per yfinance call
MAX_RETRIES = 4
INITIAL_BACKOFF = 2      # seconds; doubles each retry
PAUSE_BETWEEN_BATCHES = 1


def load_tickers() -> list[str]:
    if not TICKER_MAP_PATH.exists():
        sys.exit(
            f"ERROR: {TICKER_MAP_PATH} not found.\n"
            "Run scripts/build_ticker_universe.py first."
        )
    return sorted(json.loads(TICKER_MAP_PATH.read_text()).keys())


def download_batch(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """Download one batch with exponential backoff.

    Returns yfinance's wide multi-index frame, or an empty frame if
    every attempt failed.
    """
    backoff = INITIAL_BACKOFF

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            df = yf.download(
                tickers=tickers,
                start=start,
                end=end,
                interval="1d",
                auto_adjust=False,   # keep raw OHLC; adjustment belongs in dbt
                group_by="column",
                threads=True,
                progress=False,
            )
            if df is not None and not df.empty:
                return df
            print(f"  attempt {attempt}: empty response, retrying in {backoff}s")
        except Exception as exc:
            print(f"  attempt {attempt} failed ({type(exc).__name__}: {exc}), "
                  f"retrying in {backoff}s")

        if attempt < MAX_RETRIES:
            time.sleep(backoff)
            backoff *= 2

    print(f"  GIVING UP on batch after {MAX_RETRIES} attempts: {tickers}")
    return pd.DataFrame()


def to_long_format(df: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    """Reshape yfinance's wide frame into one row per ticker per date.

    With multiple tickers yfinance returns MultiIndex columns like
    (Open, AAPL). With a single ticker it returns flat columns. Handle
    both so a one-ticker batch doesn't silently break.
    """
    if df.empty:
        return pd.DataFrame()

    if isinstance(df.columns, pd.MultiIndex):
        long_df = (
            df.stack(level=1, future_stack=True)
            .rename_axis(index=["date", "ticker"])
            .reset_index()
        )
    else:
        long_df = df.reset_index().rename(columns={"Date": "date"})
        long_df["ticker"] = tickers[0]

    long_df.columns = [str(c).lower().replace(" ", "_") for c in long_df.columns]

    wanted = ["date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]
    present = [c for c in wanted if c in long_df.columns]
    long_df = long_df[present]

    # Rows where every price field is null are non-trading days or
    # tickers Yahoo had no data for. Drop them rather than landing nulls.
    price_cols = [c for c in ["open", "high", "low", "close"] if c in long_df.columns]
    long_df = long_df.dropna(subset=price_cols, how="all")

    long_df["date"] = pd.to_datetime(long_df["date"]).dt.date
    return long_df


def write_partitioned(df: pd.DataFrame) -> int:
    """Write one Parquet file per date: data/prices/date=YYYY-MM-DD/prices.parquet

    Hive-style partitioning lets query engines skip whole folders when
    filtering by date, and makes incremental loads append-only.
    """
    if df.empty:
        return 0

    files_written = 0
    for day, group in df.groupby("date"):
        partition_dir = OUTPUT_DIR / f"date={day}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        group.drop(columns=["date"]).to_parquet(
            partition_dir / "prices.parquet",
            index=False,
            compression="snappy",
        )
        files_written += 1
    return files_written


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest daily OHLCV to Parquet.")
    parser.add_argument("--start", default=str(date.today() - timedelta(days=365)),
                        help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", default=str(date.today()),
                        help="YYYY-MM-DD (exclusive, yfinance convention)")
    args = parser.parse_args()

    tickers = load_tickers()
    print(f"Ingesting {len(tickers)} tickers from {args.start} to {args.end}\n")

    all_frames = []
    failed_batches = []

    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        print(f"Batch {batch_num}: {len(batch)} tickers")

        raw = download_batch(batch, args.start, args.end)
        if raw.empty:
            failed_batches.append(batch)
            continue

        long_df = to_long_format(raw, batch)
        print(f"  -> {len(long_df):,} rows")
        all_frames.append(long_df)

        time.sleep(PAUSE_BETWEEN_BATCHES)

    if not all_frames:
        sys.exit("\nERROR: no data retrieved. Yahoo may be rate limiting you. "
                 "Wait a few minutes and retry.")

    combined = pd.concat(all_frames, ignore_index=True)
    files = write_partitioned(combined)

    print(f"\nWrote {len(combined):,} rows across {files} date partitions "
          f"to {OUTPUT_DIR}")
    print(f"Tickers with data: {combined['ticker'].nunique()} / {len(tickers)}")

    if failed_batches:
        flat = [t for b in failed_batches for t in b]
        print(f"\nWARNING: {len(flat)} tickers failed entirely: {flat}")


if __name__ == "__main__":
    main()