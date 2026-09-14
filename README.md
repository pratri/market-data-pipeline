# Market Data Pipeline

A batch pipeline that pulls daily stock prices and quarterly SEC filings,
lands them in S3, loads them into Snowflake, and transforms them with dbt
into a daily fact table with valuation metrics for 64 US large caps.

I built it to learn how the usual data engineering tools fit together on a
real dataset: orchestration, infrastructure as code, cloud storage, a
warehouse, and transformations with tests.

## Architecture

```
Yahoo Finance ──┐
                ├──► Airflow (EC2) ──► S3 (Parquet) ──► Snowflake ──► dbt ──► marts
SEC EDGAR ──────┘         │              partitioned      COPY INTO      tests
                          │                                 external
                     2 DAGs, different                      stage
                     schedules
```

Two Python scripts do the ingestion. One pulls daily OHLCV, dividends and
split ratios from Yahoo for a fixed list of 64 tickers, and the other pulls
XBRL company facts from SEC EDGAR. Both write Parquet to S3.

Airflow 3 runs in Docker on a t3.medium. Prices run weekday mornings before
the open. Fundamentals run on Sundays, since SEC data only changes quarterly
and pulling it daily would just be dozens of wasted API calls. The DAGs stop
at S3, and I run `COPY INTO` and `dbt build` by hand.

The bucket has versioning, encryption and lifecycle rules. Prices are
partitioned by date (`raw/prices/date=2026-09-03/`) and fundamentals are
written as dated snapshots. dbt reads each ticker's newest snapshot, so a
company SEC didn't return one week keeps last week's data.

Snowflake loads from an external stage with `COPY INTO`, reading the bucket
directly through a cross-account IAM role.

dbt has staging, intermediate and marts layers with 63 tests: 58 generic
and 5 singular tests in `dbt/tests/` that check actual values, like
split-sized returns, stale fundamentals and missing price dates.

Terraform manages the S3 bucket, both IAM roles, the EC2 instance and its
security group, so `terraform destroy` removes everything when the trial
credits run out.

### dbt model lineage

![dbt lineage](docs/lineage-graph.png)

(The screenshot is from before `int_stock_splits`, `int_market_cap_shares`
and `mart_sector_daily` were added.)

### Airflow

The two DAGs and their run history. The scheduled runs weren't triggered by
hand.

![Airflow DAGs](docs/airflow-dags.png)

![Airflow runs](docs/airflow-runs.png)

## Design decisions

### Joining fundamentals to prices by filing date

A company's Q2 ends June 30 but doesn't get reported until early August. If
fundamentals are joined to prices on period end, every July row gets a P/E
computed from earnings nobody knew about yet. That's lookahead bias.

So the join is as-of filing date: for each trading day, take the newest
quarter whose numbers were all filed on or before that day. At first I had
a test that `days_since_filing` is never negative and assumed that covered
it. It didn't (see the first finding below).

### Filtering flow metrics by period duration

SEC reports the same metric over overlapping periods that share an end
date. Q2 (Apr-Jun) and the half year (Jan-Jun) both end June 30, so grouping
on end date alone roughly doubles revenue.

Every fundamentals row gets classified by the gap between its start and end
dates: quarterly, half year, nine month, annual, or instant for balance
sheet items. The buckets are wide enough for Costco and PepsiCo, whose
fiscal quarters run 12, 12, 12 and 16 weeks.

### Prices as traded, share counts as filed

Yahoo adjusts historical prices for splits as of the day you download them,
while SEC share counts are as of the day they were filed. If a split
happened in between, market cap is off by the split ratio. So prices are
converted back to what they traded at, and share counts are adjusted for
any split between the filing and the trade date.

## Things I found by looking at the data

All the tests passed on a version where 98% of the valuation ratios were
null, and on a later version where a third of the fact table had year-old
fundamentals. I found these by querying the output. The queries are in
`audit/warehouse_audit.sql`.

The biggest one: ingestion kept whichever filing most recently repeated a
number, and every 10-Q and 10-K restates prior quarters as comparatives, so
the filed date kept moving forward. Apple's June 2024 quarter ended up dated
August 2025, and on 2025-09-11 the fact table showed $85.8bn of revenue
instead of the real $94.0bn. From October 2025 to May 2026 it was using a
2023 quarter, 37% of rows were over a year stale, and the staleness test
still passed because it used the same wrong dates. The fix was keeping the
first filing of each number instead of the latest.

Netflix looked like a $50bn company for ten months. Yahoo's price history is
split-adjusted after the fact, so its ~$120 September 2025 close got
multiplied by the ~425M pre-split share count from the 10-Q, and P/E sat
around 5. A couple of Yahoo's "splits" are really spinoffs, which move the
price but not the share count, so I told them apart by comparing cover page
share counts before and after each event.

McDonald's market cap once came out in the hundreds of thousands of dollars,
because it files weighted average shares in millions and the model read that
as a raw count. Cover page counts can't be trusted blindly either:
Mastercard's cover page only lists one share class, 122.5M against roughly
876M outstanding, and Visa and UPS have the same problem. A cover count is
now only used when a separate us-gaap count in the same filing agrees within
10%. Checked against Yahoo for all 64 tickers afterward, 61 agree within
0.2%, and the three that don't are MA, V and UPS.

Banks and brokers don't report revenue like other companies. Goldman Sachs
has zero revenue rows across 78 quarters, so price-to-sales is left null for
the eight of them (JPM, BAC, WFC, C, GS, MS, SCHW, AXP). The other Financials
report revenue normally. Price-to-book works for all of them.

A few smaller things broke too: GE tagged a $2.585bn line for Q4 2015 that
isn't total revenue, NVDA lost four years of revenue when it switched XBRL
tags, and Costco and PepsiCo's 16-week fourth quarters fell outside the
window used to classify filing periods, so I widened it.

## Dashboard

`mart_sector_daily` has one row per sector per trading day. It has
cap-weighted P/E, P/S and P/B (total market cap over the total of the other
side, loss makers included), medians, and `companies_missing_market_cap`,
which shows when a sector total is missing a company.

I didn't use averages of the per-company ratios. `AVG(pe_ratio)` drops every
loss maker and lets one outlier move the whole sector: on the last day of
the sample Consumer Discretionary averaged 103 against a median of 22. The
dashboard also shows ticker counts next to the sector numbers, since a
4-company sector isn't comparable to a 15-company one.

Live dashboard: [Tableau Public](https://public.tableau.com/app/profile/pranav.tripuraneni/viz/Book1_17892343778990/SectorValuationComparisonUSLarge-CapEquities)

![Sector valuation dashboard](docs/dashboard.png)

The published version runs off a flat export (`tableau/market_metrics.csv`,
16,000 rows across the 64 tickers) because Tableau Public can't connect to
Snowflake. A couple of tickers (MCD, ABBV) show a blank P/B because they
have negative stockholders' equity.

Packaged workbook: [`tableau/sector_valuation_dashboard.twbx`](tableau/sector_valuation_dashboard.twbx)

## Stack

Python, Airflow 3, Docker, AWS (S3, EC2, IAM), Terraform, Snowflake, dbt,
GitHub Actions.

## Layout

```
dags/                 Airflow DAGs
scripts/              ingestion + shared S3 helpers
dbt/models/           staging → intermediate → marts (incl. mart_sector_daily)
dbt/tests/            singular data tests
tests/                DuckDB model run + ingestion unit tests, with fixtures
audit/                queries for checking the warehouse output by hand
terraform/            S3, IAM, EC2, security group, Snowflake integration role
snowflake_setup.sql   warehouse, stage and COPY INTO setup
tableau/              export for the dashboard
.github/              CI
```

## CI

Pushes and PRs to main run Python linting, an Airflow DAG import check and
`dbt parse`. None of it needs warehouse credentials. `dbt parse` only
resolves refs and Jinja and never runs the SQL, so CI also does:

- `sqlfluff parse` with the Snowflake dialect. It catches syntax errors, but
  not functions that exist in other warehouses and not in Snowflake.
- `tests/run_models_duckdb.py`, which builds every model on DuckDB over 21
  tickers of real SEC and Yahoo data in `tests/fixtures/` (335 KB). It runs
  the tests from the .yml files and `dbt/tests/`, then checks values I know
  the answer to: headline revenue for 10 companies, market cap across
  Netflix's split, which quarter each trading day uses, Visa having no
  market cap and McDonald's having one.
- `tests/test_ingestion.py`, unit tests for extraction rules the fixtures
  can't cover (first filing wins, form filtering, the largest-tag rule),
  since the fixtures were produced by ingestion.

Both suites add made-up cases the fixtures don't have, like a fact tagged
years late, a cover count for one share class and a missing quarter.
Without them, reverting those fixes would still pass. I reverted each fix
one at a time and all 16 broke at least one test.

DuckDB isn't Snowflake, so the SQL gets rewritten where the dialects differ.
This catches logic and value bugs, not dialect problems.

Locally:

```bash
pip install duckdb pandas pyarrow pyyaml requests boto3
python tests/test_ingestion.py
python tests/run_models_duckdb.py
```

A full `dbt build` against Snowflake is a separate job that only runs when
triggered by hand, because the warehouse is on trial credits.

## Known limitations

- Values are as originally reported. Ingestion keeps the first filing of
  each number, so later restatements are ignored. Around a spinoff, quarters
  before and after are on different bases, so a TTM figure that spans one
  mixes both (Honeywell's P/E right after its 2026 separation includes
  earnings from the business it spun off).
- SEC's companyfacts API is missing some filings. Citigroup's 2026 10-Qs and
  S&P Global's FY2024 10-K have XBRL but weren't in the API as of September
  2026, so Citi's fundamentals go stale and S&P Global has no TTM for five
  months.
- Visa has no market cap. It only reports share counts per class, and
  companyfacts leaves out anything broken down by class. GOOGL and META have
  the same cover page problem but fall back to weighted average shares.
- Splits before the loaded price history aren't known, so share counts filed
  more than 400 days before a trade date aren't used.
- Net income is as reported, one-offs included. Alphabet's Q2 2026 10-Q tags
  $112.2bn of net income on $119.8bn of revenue against $40.8bn of operating
  income, so trailing P/E and net margin move with non-operating items too.
- Sector mapping is a hardcoded list of 64 tickers rather than pulled from
  SEC or GICS.
- yfinance is an unofficial scraper. It rate limits and breaks when Yahoo
  changes their site.
- It all runs on one EC2 box with LocalExecutor, which is fine for two DAGs
  and 64 tickers but not how you'd run anything bigger.

## Running it

Needs an AWS account, a Snowflake account, Terraform, Docker and Python.

```bash
pip install -r requirements.txt
cp .env.example .env                                  # fill in
cp terraform/terraform.tfvars.example terraform/terraform.tfvars   # fill in
cp dbt/profiles.yml.example ~/.dbt/profiles.yml       # fill in

cd terraform && terraform init && terraform apply     # S3, IAM, EC2
```

Then run `snowflake_setup.sql` section by section to create the warehouse,
storage integration, stages and tables. The storage integration takes two
passes: Terraform creates the IAM role, Snowflake generates the principal
and external ID that go in its trust policy, then Terraform applies again.

```bash
python scripts/build_ticker_universe.py
python scripts/ingest_prices.py
python scripts/ingest_fundamentals.py
```

Run the `COPY INTO` statements (steps 6 and 7), then:

```bash
cd dbt && dbt deps && dbt build
```

For Airflow on EC2, clone the repo onto the instance, remove the local AWS
credentials mount from `docker-compose.yaml` (the instance profile provides
credentials), copy `.env.airflow.example` to `.env.airflow` and fill it in,
then:

```bash
docker compose --env-file .env.airflow up -d
```

### Deployment notes

Things that cost me time on the box:

- A t3.small won't run this. 2 GB isn't enough for Airflow 3's four services
  plus a task, so the scheduler goes unhealthy and tasks fail with connection
  refused once memory runs out. t3.medium (4 GB) is the minimum and the
  Terraform default.
- Airflow 3 workers talk to the API server over HTTP instead of writing to
  the database, so `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` has to point at
  the apiserver container, or the worker tries localhost and fails.
- Workers also need a shared `AIRFLOW__API_AUTH__JWT_SECRET`. Without it each
  container makes its own, and tasks sit in `queued` with "Signature
  verification failed".
- Docker creates missing mount directories as root, so create the logs
  directory and give it to the Airflow user before starting, or the
  dag-processor can't write logs and parses nothing. On Linux, set
  `AIRFLOW_UID=1000` (your `id -u`) in `.env.airflow` and run:

  ```bash
  mkdir -p logs && sudo chown -R 1000:0 logs
  ```

- Airflow 3 replaced the old user table with SimpleAuthManager, so there's
  no admin/admin login. The generated password is in
  `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`:

  ```bash
  docker compose exec apiserver cat /opt/airflow/simple_auth_manager_passwords.json.generated
  ```
