# Market Data Pipeline

A batch pipeline that pulls daily stock prices and quarterly SEC filings,
lands them in S3, loads them into Snowflake, and transforms them with dbt
into a daily fact table with valuation metrics.

Built to practice the modern data stack end to end: orchestration,
infrastructure as code, cloud storage, a warehouse, and transformation
with tests.

## Architecture

```
Yahoo Finance ──┐
                ├──► Airflow (EC2) ──► S3 (Parquet) ──► Snowflake ──► dbt ──► marts
SEC EDGAR ──────┘         │              partitioned      COPY INTO      tests
                          │                                 external
                     2 DAGs, different                      stage
                     schedules
```

**Ingestion.** Two Python scripts. One pulls daily OHLCV from Yahoo for a
fixed 64-ticker universe, the other pulls XBRL company facts from SEC
EDGAR. Both write Parquet to S3.

**Orchestration.** Airflow 3 running in Docker on a t3.small. Prices run
weekday mornings, fundamentals run Sundays, because SEC data only changes
quarterly and polling it daily would be 64 pointless API calls.

**Storage.** S3 with versioning, encryption, and lifecycle rules. Prices
are partitioned by date (`raw/prices/date=2026-09-03/`), fundamentals are
written as dated snapshots so you can see what SEC reported at different
points in time.

**Warehouse.** Snowflake, loaded via `COPY INTO` from an external stage.
Snowflake assumes a cross-account IAM role to read the bucket directly,
so no data passes through an intermediate process.

**Transformation.** dbt with staging, intermediate and marts layers.
41 tests.

**Infrastructure.** Terraform manages the S3 bucket, both IAM roles, the
EC2 instance and its security group. `terraform destroy` tears the whole
thing down, which matters when you're running it on trial credits.

### dbt model lineage

![dbt lineage](docs/lineage-graph.png)

### Airflow

Both DAGs on their own schedules.

![Airflow DAGs](docs/airflow-dags.png)

Run history. The scheduled ones fired on their own.

![Airflow runs](docs/airflow-runs.png)

## The two decisions that mattered

### Joining fundamentals to prices by filing date

A company's Q2 ends June 30 but doesn't get reported until early August.
If you join fundamentals to prices on period end, every July row gets a
P/E ratio computed from earnings nobody knew about yet. That's lookahead
bias, and it's the reason a lot of backtests look great and then don't
work.

So the join is as-of filing date: for each trading day, take the most
recent filing on or before that day. There's a test asserting
`days_since_filing` is never negative, which is what proves it.

### Filtering flow metrics by period duration

SEC reports the same metric over overlapping durations that share an end
date. Q2 alone (Apr–Jun) and the half-year (Jan–Jun) both end June 30.
Sum them without noticing and you roughly double revenue.

Every fundamentals row gets classified by the gap between its start and
end dates — quarterly, half-year, nine-month, annual, or instant for
balance-sheet items that have no duration at all. Flow metrics filter to
quarterly, balance items to instant.

## Things I found by looking at the data

The tests all passed on a version of this where 98% of the valuation
ratios were null. Tests check what you thought to check. These came from
querying the output and noticing the numbers were wrong.

**Companies switch XBRL tags mid-history.** This one was invisible until
I checked coverage by year. NVDA reported revenue under
`RevenueFromContractWithCustomerExcludingAssessedTax` until January 2022
— 28 entries — and has used `Revenues` ever since, 280 entries. Both
tags were in my priority list, but my extraction returned on the first
tag that had *any* data. So NVDA got 28 rows ending in 2022 and nothing
after, which meant no net margin and no price-to-sales for four years of
the fact table.

Same bug in mirror image for CAT, which had revenue but no net income,
and for PFE, GOOGL, CVS and GE. Six major companies with silent holes,
and every test passing the whole time.

The fix reads every candidate tag and merges them, deduplicating on
period and resolving overlaps to the higher-priority tag. That took
revenue coverage from 12,674 rows to 14,721, and price-to-sales from
8,237 to 12,364.

**Share counts are only reported annually.** Most filers include
`CommonStockSharesOutstanding` in their 10-K and omit it from
quarterlies — NVDA reports it at its January fiscal year end and nowhere
else. That left 12 of 64 companies with no share count on the most
recent quarter, and no shares means no market cap, so no P/E, no
price-to-book, no price-to-sales. Carrying the last known count forward
fixes it, with `shares_basis` marking which rows are carried forward.
The fill is unbounded, and in practice a carried-forward count averages
789 days stale and reaches 949 at worst, so market caps built on those
rows are approximations rather than facts.

**Differencing across a tag switch produced negative revenue.** Once I
merged tags, a company's cumulative series for one fiscal year could
span two of them. MA reported Q1 through Q3 2021 under `Revenues` and
the full year under `SalesRevenueNet`, a narrower concept. Subtracting
the 11.1bn annual figure from the 13.7bn nine-month figure gave -2.589bn
of "Q4 revenue". Twenty-odd companies had a mixed-tag series, so it
wasn't a one-off.

Fixed by partitioning the cumulative series by tag as well as period
start, so differencing stays inside one series. A transition year now
loses its derived quarters rather than inventing wrong ones. There's a
test asserting revenue is never negative, since that's a value which
can't legitimately occur and is a good canary for this class of bug.

**Shares outstanding comes in two shapes.** About half the universe
reports `CommonStockSharesOutstanding`, a count at a point in time. The
rest report `WeightedAverageNumberOfDilutedSharesOutstanding`, which is
an average over the period and therefore carries a start date, so it
classifies as a duration metric rather than an instant one. My first
version only took the instant form, which meant half the companies had
no share count at all. Fixed by accepting both and keeping a
`shares_basis` column so you can tell which one a row used.

**Banks don't report revenue.** Goldman Sachs has zero revenue rows
across 78 quarters. Morgan Stanley has one. That isn't a gap in my tag
list, it's that "revenue" isn't a concept that maps onto a bank — they
report interest income, non-interest income, and revenues net of interest
expense. I could have added those tags and filled the column, but then
price-to-sales for Goldman would be a number that looks fine and means
nothing next to the same ratio for Costco. So financials are flagged and
excluded from revenue-based ratios. Price-to-book works fine for them and
is populated.

**Some companies never report a standalone quarter.** They file Q1, then
year-to-date H1, 9M and FY. Taking only `period_type = 'quarterly'`
dropped most of ABBV's history. Discrete quarters are derived by
differencing consecutive cumulative figures (Q2 = H1 − Q1, and so on),
with the derived values tagged `revenue_source = 'derived'` so they're
distinguishable from what companies actually stated. This recovered 361
quarters — smaller than I expected, because most companies that file
cumulatively also file the standalone quarter, and the as-reported value
wins the tiebreak.

**One ticker has almost no history.** XOM's current CIK is a recently
registered entity following a corporate reorganization, so its SEC
filings only go back to 2024 — 6 quarters against 70-odd for everyone
else. It's flagged rather than dropped, because a documented gap is more
useful than a silently missing company.

## Stack

Python, Airflow 3, Docker, AWS (S3, EC2, IAM), Terraform, Snowflake, dbt,
GitHub Actions.

## Layout

```
dags/          Airflow DAGs
scripts/       ingestion + shared S3 helpers
dbt/models/    staging → intermediate → marts
terraform/     S3, IAM, EC2, security group, Snowflake integration role
snowflake/     warehouse, stage and COPY INTO setup
.github/       CI
```

## CI

Every push runs Python linting, an Airflow DAG import check, and
`dbt parse` to resolve the model graph. None of that needs warehouse
credentials, so it stays fast and free.

A full `dbt build` against Snowflake is a separate manually-triggered
job. It's manual because the warehouse runs on trial credits and a
workflow that starts failing when the trial expires would leave a
permanent red X on the repo.

## Known limitations

**Restatements overwrite originals.** Ingestion keeps the most recently
filed value for each period, so when a later 10-K revises a figure the
original is lost. Strictly correct point-in-time data would preserve what
was known at each moment, which means keeping the full restatement
history instead of deduplicating it. The dated S3 snapshots are a
partial version of this, but the warehouse only reads the latest.

**Sector mapping is hardcoded.** 64 tickers in a `values` list. A real
system would pull SIC codes from SEC's submissions endpoint or use GICS.

**yfinance is unofficial.** It scrapes Yahoo rather than using a licensed
API, so it rate limits and breaks when Yahoo changes their site. There's
retry logic with exponential backoff, but it's a real fragility.

**Weighted-average share counts make market cap approximate** for the
companies that use them. `shares_basis` tells you which rows those are.

**Single-node deployment.** LocalExecutor on one EC2 box. Fine for two
DAGs and 64 tickers, not what you'd run for anything real.

## Running it

Needs an AWS account, a Snowflake account, Terraform, Docker and Python.

```bash
cd terraform && terraform apply    # provision S3, IAM, EC2
```

Then run the SQL in `snowflake/` to create the warehouse, storage
integration and stages. The storage integration is a two-pass setup:
Terraform creates the IAM role, Snowflake generates the principal and
external ID that go in its trust policy, then Terraform applies again.

```bash
python scripts/build_ticker_universe.py
python scripts/ingest_prices.py
python scripts/ingest_fundamentals.py
cd dbt && dbt deps && dbt build
```

For the EC2 deployment, clone onto the instance, drop the local AWS
credentials mount from `docker-compose.yaml` (the instance profile
supplies credentials automatically), and `docker compose up -d`.

### Deployment notes

Three things that cost me time on the box and aren't obvious from the
docs.

**A t3.small can't run this.** 2 GB isn't enough for Airflow 3's four
services plus a task subprocess. It came up fine and then the first
scheduled run hung: the scheduler went unhealthy, tasks failed with
`httpx.ConnectError: Connection refused` trying to reach the API server,
and the scheduler couldn't find the DAG in `serialized_dag`. All of it
traced back to the box sitting at ~300 MB available with half a gig in
swap. t3.medium (4 GB) is the floor.

**Airflow 3 workers need the API server URL.** Tasks talk back over HTTP
now instead of writing to the database directly, so
`AIRFLOW__CORE__EXECUTION_API_SERVER_URL` has to point at the apiserver
container. Without it the worker resolves to localhost and gets
connection refused.

**And a shared JWT secret.** Workers authenticate to the API server with
a signed token. If `AIRFLOW__API_AUTH__JWT_SECRET` isn't set, every
container generates its own on startup, so the signature never verifies
and tasks sit in `queued` forever. The error is
`Invalid auth token: Signature verification failed`, buried in a
tenacity retry traceback, alongside a misleading
`not found in serialized_dag table` that sends you looking at DAG
parsing instead of auth.

**Create and chown the logs directory before starting.** Docker creates
missing mount directories as root, so the dag-processor can't write its
per-file logs and silently parses nothing — `airflow dags list` just
returns "No data found" with no import errors to explain it.

```bash
mkdir -p logs && sudo chown -R 1000:0 logs
```

**Login isn't admin/admin.** Airflow 3 replaced the old user table with
SimpleAuthManager, so `airflow users create` doesn't exist unless you
install and configure the FAB provider. The generated password is in
`$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`:

```bash
docker compose exec apiserver cat /opt/airflow/simple_auth_manager_passwords.json.generated
```
