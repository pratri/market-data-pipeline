"""
Daily OHLCV ingestion.

Runs on weekday mornings, pulls a short trailing window, and skips dates
already in S3. The trailing window rather than "just yesterday" covers
the case where a previous run failed: the next run backfills the gap
without anyone intervening.

Schedule is 6am Eastern on weekdays. Markets close at 4pm ET and Yahoo
settles the day's data overnight, so a morning run gets a complete
previous session.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

# Ten days of lookback. Long enough to cover a long weekend plus a few
# failed runs; short enough that the request stays cheap. The skip
# logic means re-fetched dates cost nothing beyond the download.
LOOKBACK_DAYS = 10


@dag(
    dag_id="ingest_prices_daily",
    description="Pull daily OHLCV from Yahoo and land partitioned Parquet in S3",
    schedule="0 6 * * 1-5",              # 6am, Mon-Fri
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    catchup=False,                        # don't backfill every day since start_date
    max_active_runs=1,                    # never let two runs write the same keys
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(minutes=30),
    },
    tags=["ingestion", "prices", "s3"],
)
def ingest_prices_daily():

    @task
    def ingest() -> dict:
        """Fetch recent prices and write any dates not already in S3.

        Imports happen inside the task rather than at module level.
        Airflow parses every DAG file on a loop, and a heavy import at
        module scope runs on every parse, slowing the whole scheduler.
        """
        import sys

        sys.argv = [
            "ingest_prices.py",
            "--days", str(LOOKBACK_DAYS),
            "--skip-existing",
        ]

        import ingest_prices

        ingest_prices.main()
        return {"lookback_days": LOOKBACK_DAYS}

    ingest()


ingest_prices_daily()
