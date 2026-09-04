"""
Weekly SEC fundamentals ingestion.

Companies file 10-Qs and 10-Ks on a quarterly cadence, so daily polling
would make 64 API calls to find nothing changed. Weekly picks up new
filings within a few days of publication without hammering SEC.

Each run writes a dated snapshot rather than overwriting, so the
transformation layer can see how a company's reported figures for a
given period changed over time. That's not incidental: restatements are
real and being able to show them is a genuine feature.
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
        # SEC's rate limit means 64 sequential calls with a deliberate
        # delay. Generous timeout so a slow response doesn't kill a run
        # that would otherwise succeed.
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
