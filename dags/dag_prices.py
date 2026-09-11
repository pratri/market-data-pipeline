"""
Daily price ingestion.

Runs 6am ET on weekdays, after Yahoo has settled the previous session.
Pulls the last 10 days and skips dates already in S3, so if a run fails
the next one fills the gap.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

# covers a long weekend plus a couple of failed runs
LOOKBACK_DAYS = 10


@dag(
    dag_id="ingest_prices_daily",
    description="Pull daily OHLCV from Yahoo and land partitioned Parquet in S3",
    schedule="0 6 * * 1-5",              # 6am Mon-Fri
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    catchup=False,                        # no backfill from start_date
    max_active_runs=1,                    # two runs shouldn't write the same keys
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

        Imports are inside the task so the scheduler doesn't run them on
        every DAG parse.
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
