"""
Daily OHLCV from Yahoo Finance, written to S3 as one parquet file per date.

yfinance scrapes Yahoo and fails now and then, so downloads retry with
backoff. Rows also carry dividends, split ratios and when they were
fetched. Yahoo's close is split-adjusted as of the download, so dbt needs
the fetch time to put rows downloaded before a split on the same basis.

--skip-existing leaves dates already in S3 alone, except to add tickers a
file is missing, so one failed batch doesn't leave a permanent hole.

Usage:
    python scripts/ingest_prices.py
    python scripts/ingest_prices.py --start 2024-01-01 --end 2024-06-30
    python scripts/ingest_prices.py --days 5 --skip-existing
"""

import argparse
import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

sys.path.insert(0, str(Path(__file__).parent))
from s3_utils import get_bucket, list_s3_keys, read_parquet_from_s3, write_parquet_to_s3

PROJECT_ROOT = Path(__file__).parent.parent
TICKER_MAP_PATH = PROJECT_ROOT / "data" / "ticker_cik_map.json"

S3_PREFIX = "raw/prices"
FETCHED_AT_FORMAT = "%Y-%m-%d %H:%M:%S"   # UTC

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


def partition_key(day_str: str) -> str:
    return f"{S3_PREFIX}/date={day_str}/prices.parquet"


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
                actions=True,        # adds Dividends and Stock Splits
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

    wanted = [
        "date", "ticker", "open", "high", "low", "close", "adj_close", "volume",
        "dividends", "stock_splits",
    ]
    present = [c for c in wanted if c in long_df.columns]
    long_df = long_df[present]

    price_cols = [c for c in ["open", "high", "low", "close"] if c in long_df.columns]
    long_df = long_df.dropna(subset=price_cols, how="all")

    long_df["date"] = pd.to_datetime(long_df["date"]).dt.date
    return long_df


def upload_partitions(df: pd.DataFrame, bucket: str, existing: set[str]) -> tuple[int, int, int]:
    """Write one parquet file per date. Returns (written, refilled, skipped).

    A date already in S3 is only rewritten to add tickers its file is
    missing. Rows already in the file are kept as they are.
    """
    if df.empty:
        return 0, 0, 0

    written = refilled = skipped = 0
    for day, group in df.groupby("date"):
        day_str = str(day)
        key = partition_key(day_str)
        group = group.drop(columns=["date"])   # date is in the key

        if day_str in existing:
            current, last_modified = read_parquet_from_s3(key, bucket=bucket)
            missing = group[~group["ticker"].isin(current["ticker"])]
            if missing.empty:
                skipped += 1
                continue

            # files from before fetched_at existed: upload time is the best guess
            upload_time = last_modified.astimezone(timezone.utc).strftime(FETCHED_AT_FORMAT)
            if "fetched_at" in current.columns:
                current["fetched_at"] = current["fetched_at"].fillna(upload_time)
            else:
                current["fetched_at"] = upload_time

            group = pd.concat([current, missing], ignore_index=True)
            refilled += 1
        else:
            written += 1

        write_parquet_to_s3(group, key, bucket=bucket)

    return written, refilled, skipped


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest daily OHLCV to S3.")
    parser.add_argument("--start", help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", help="YYYY-MM-DD (exclusive, yfinance convention)")
    parser.add_argument("--days", type=int,
                        help="Shorthand: last N days. Overrides --start.")
    parser.add_argument("--skip-existing", action="store_true",
                        help="Leave dates already in S3 alone, apart from adding missing tickers.")
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

    existing = existing_dates(bucket) if args.skip_existing else set()
    if existing:
        print(f"Found {len(existing)} dates already in S3, only missing tickers get added to those.\n")

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
        long_df["fetched_at"] = datetime.now(timezone.utc).strftime(FETCHED_AT_FORMAT)
        print(f"  -> {len(long_df):,} rows")
        all_frames.append(long_df)

        time.sleep(PAUSE_BETWEEN_BATCHES)

    if not all_frames:
        sys.exit("\nERROR: no data retrieved. Yahoo may be rate limiting you. "
                 "Wait a few minutes and retry.")

    combined = pd.concat(all_frames, ignore_index=True)
    print(f"\nUploading {len(combined):,} rows...")

    written, refilled, skipped = upload_partitions(combined, bucket, existing)

    print(f"\nWrote {written} new date partitions to s3://{bucket}/{S3_PREFIX}")
    if refilled:
        print(f"Added missing tickers to {refilled} existing dates.")
    if skipped:
        print(f"Skipped {skipped} dates already complete.")
    print(f"Tickers with data: {combined['ticker'].nunique()} / {len(tickers)}")

    if failed_batches:
        flat = [t for b in failed_batches for t in b]
        print(f"\nWARNING: {len(flat)} tickers failed entirely: {flat}")


if __name__ == "__main__":
    main()
