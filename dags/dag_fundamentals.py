"""
Weekly SEC fundamentals ingestion.

Filings come out quarterly, so weekly picks them up within a few days
without hitting SEC for nothing. Each run writes a new dated snapshot.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="ingest_fundamentals_weekly",
    description="Pull SEC XBRL fundamentals and land a dated snapshot in S3",
    schedule="0 7 * * 0",                 # 7am Sunday
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=15),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(hours=1),
    },
    tags=["ingestion", "fundamentals", "sec", "s3"],
)
def ingest_fundamentals_weekly():

    @task(
        # 64 calls with a delay between each, so give it room
        execution_timeout=pendulum.duration(minutes=30),
    )
    def ingest() -> dict:
        import sys

        sys.argv = ["ingest_fundamentals.py"]

        import ingest_fundamentals

        ingest_fundamentals.main()
        return {"status": "complete"}

    ingest()


ingest_fundamentals_weekly()
