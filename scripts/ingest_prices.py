"""
Daily OHLCV from Yahoo Finance, written to S3 as one parquet file per date.

yfinance scrapes Yahoo and fails now and then, so downloads retry with
backoff. --skip-existing skips dates already in S3.

Usage:
    python scripts/ingest_prices.py
    python scripts/ingest_prices.py --start 2024-01-01 --end 2024-06-30
    python scripts/ingest_prices.py --days 5 --skip-existing
"""

import argparse
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

sys.path.insert(0, str(Path(__file__).parent))
from s3_utils import get_bucket, list_s3_keys, write_parquet_to_s3

PROJECT_ROOT = Path(__file__).parent.parent
TICKER_MAP_PATH = PROJECT_ROOT / "data" / "ticker_cik_map.json"

S3_PREFIX = "raw/prices"

BATCH_SIZE = 20
MAX_RETRIES = 4
INITIAL_BACKOFF = 2
PAUSE_BETWEEN_BATCHES = 1


def load_tickers() -> list[str]:
    if not TICKER_MAP_PATH.exists():
        sys.exit(
            f"ERROR: {TICKER_MAP_PATH} not found.\n"
            "Run scripts/build_ticker_universe.py first."
        )
    return sorted(json.loads(TICKER_MAP_PATH.read_text()).keys())


def existing_dates(bucket: str) -> set[str]:
    """Dates already in S3, parsed from keys like raw/prices/date=2024-01-02/prices.parquet."""
    dates = set()
    for key in list_s3_keys(f"{S3_PREFIX}/", bucket=bucket):
        for part in key.split("/"):
            if part.startswith("date="):
                dates.add(part.removeprefix("date="))
    return dates


def download_batch(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """Download one batch, retrying with exponential backoff."""
    backoff = INITIAL_BACKOFF

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            df = yf.download(
                tickers=tickers,
                start=start,
                end=end,
                interval="1d",
                auto_adjust=False,   # raw OHLC plus adj_close (close is still split-adjusted)
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
    """Reshape yfinance's wide frame to one row per ticker per date.

    Several tickers come back with (field, ticker) columns, a single ticker
    comes back flat. Both are handled.
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

    price_cols = [c for c in ["open", "high", "low", "close"] if c in long_df.columns]
    long_df = long_df.dropna(subset=price_cols, how="all")

    long_df["date"] = pd.to_datetime(long_df["date"]).dt.date
    return long_df


def upload_partitions(df: pd.DataFrame, bucket: str, skip: set[str]) -> tuple[int, int]:
    """Write one parquet file per date. Returns (written, skipped)."""
    if df.empty:
        return 0, 0

    written = skipped = 0
    for day, group in df.groupby("date"):
        day_str = str(day)
        if day_str in skip:
            skipped += 1
            continue

        key = f"{S3_PREFIX}/date={day_str}/prices.parquet"
        # date is in the key, no need to store it in the file
        write_parquet_to_s3(group.drop(columns=["date"]), key, bucket=bucket)
        written += 1

    return written, skipped


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest daily OHLCV to S3.")
    parser.add_argument("--start", help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", help="YYYY-MM-DD (exclusive, yfinance convention)")
    parser.add_argument("--days", type=int,
                        help="Shorthand: last N days. Overrides --start.")
    parser.add_argument("--skip-existing", action="store_true",
                        help="Skip dates already present in S3.")
    args = parser.parse_args()

    end = args.end or str(date.today())
    if args.days:
        start = str(date.today() - timedelta(days=args.days))
    else:
        start = args.start or str(date.today() - timedelta(days=365))

    bucket = get_bucket()
    tickers = load_tickers()

    print(f"Bucket:  s3://{bucket}/{S3_PREFIX}")
    print(f"Window:  {start} to {end}")
    print(f"Tickers: {len(tickers)}\n")

    skip = existing_dates(bucket) if args.skip_existing else set()
    if skip:
        print(f"Found {len(skip)} dates already in S3, will skip those.\n")

    all_frames = []
    failed_batches = []

    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        print(f"Batch {i // BATCH_SIZE + 1}: {len(batch)} tickers")

        raw = download_batch(batch, start, end)
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
    print(f"\nUploading {len(combined):,} rows...")

    written, skipped = upload_partitions(combined, bucket, skip)

    print(f"\nWrote {written} date partitions to s3://{bucket}/{S3_PREFIX}")
    if skipped:
        print(f"Skipped {skipped} dates already present.")
    print(f"Tickers with data: {combined['ticker'].nunique()} / {len(tickers)}")

    if failed_batches:
        flat = [t for b in failed_batches for t in b]
        print(f"\nWARNING: {len(flat)} tickers failed entirely: {flat}")


if __name__ == "__main__":
    main()
