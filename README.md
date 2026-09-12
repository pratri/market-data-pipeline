# Market Data Pipeline

A batch pipeline that pulls daily stock prices and quarterly SEC filings,
lands them in S3, loads them into Snowflake, and transforms them with dbt
into a daily fact table with valuation metrics for 64 US large caps.

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

**Ingestion.** Two Python scripts. One pulls daily OHLCV, dividends and
split ratios from Yahoo for a fixed 64-ticker universe, the other pulls
XBRL company facts from SEC EDGAR. Both write Parquet to S3.

**Orchestration.** Airflow 3 running in Docker on a t3.medium. Prices run
weekday mornings before the open, fundamentals run Sundays, because SEC
data only changes quarterly and polling it daily would be 64 pointless API
calls. The DAGs stop at S3; `COPY INTO` and `dbt build` are run by hand.

**Storage.** S3 with versioning, encryption, and lifecycle rules. Prices
are partitioned by date (`raw/prices/date=2026-09-03/`), fundamentals are
written as dated snapshots. dbt reads each ticker's newest snapshot, so a
company SEC didn't return one week keeps last week's data.

**Warehouse.** Snowflake, loaded via `COPY INTO` from an external stage.
Snowflake assumes a cross-account IAM role to read the bucket directly,
so no data passes through an intermediate process.

**Transformation.** dbt with staging, intermediate and marts layers.
63 tests: 58 generic, plus 5 singular tests in `dbt/tests/` that check
the output makes sense rather than just its shape.

**Infrastructure.** Terraform manages the S3 bucket, both IAM roles, the
EC2 instance and its security group. `terraform destroy` tears the whole
thing down, which matters when you're running it on trial credits.

### dbt model lineage

![dbt lineage](docs/lineage-graph.png)

(The screenshot predates `int_stock_splits`, `int_market_cap_shares` and
`mart_sector_daily`.)

### Airflow

Both DAGs on their own schedules.

![Airflow DAGs](docs/airflow-dags.png)

Run history. The scheduled ones fired on their own.

![Airflow runs](docs/airflow-runs.png)

## The decisions that mattered

### Joining fundamentals to prices by filing date

A company's Q2 ends June 30 but doesn't get reported until early August.
If you join fundamentals to prices on period end, every July row gets a
P/E ratio computed from earnings nobody knew about yet. That's lookahead
bias, and it's the reason a lot of backtests look great and then don't
work.

So the join is as-of filing date: for each trading day, take the newest
quarter whose numbers were all filed on or before that day. I used to say
a test asserting `days_since_filing` is never negative proved this. It
didn't, see the first finding below.

### Filtering flow metrics by period duration

SEC reports the same metric over overlapping durations that share an end
date. Q2 alone (Apr–Jun) and the half-year (Jan–Jun) both end June 30.
Sum them without noticing and you roughly double revenue.

Every fundamentals row gets classified by the gap between its start and
end dates: quarterly, half-year, nine-month, annual, or instant for
balance-sheet items that have no duration at all. The buckets are wide
enough for Costco and PepsiCo, whose fiscal quarters run 12, 12, 12 and 16
weeks.

### Prices as traded, share counts as filed

Yahoo adjusts historical prices for splits as of the day you download
them. SEC share counts are as of the day they were filed. Multiply one by
the other without lining them up and market cap is off by the split ratio.
Prices get put back to what they actually traded at, and share counts get
multiplied by any split between their filing and the trade date.

## Things I found by looking at the data

The tests all passed on a version of this where 98% of the valuation
ratios were null, and again on a version where a third of the fact table
carried year-old fundamentals. Tests check what you thought to check.
These came from querying the output and noticing the numbers were wrong.
The queries are in `audit/warehouse_audit.sql`.

**Filed dates drifted forward.** Every 10-Q and 10-K repeats earlier
periods as comparatives, and ingestion kept the most recently filed copy
of each number. So Apple's June 2024 quarter had a filed date of August
2025, when the next year's 10-Q repeated it, and the as-of join couldn't
use it until then. On 2025-09-11 AAPL showed $85.8bn of revenue (June
2024) instead of $94.0bn (June 2025). From October 2025 to May 2026 it
carried a 2023 quarter. 37% of fact table rows had fundamentals over a
year old, and `days_since_filing` looked normal the whole time because it
was computed from the same wrong dates. Ingestion now keeps the first
filing of each number, and two singular tests check the attached quarter
is the newest one available and not stale.

**A 10-for-1 split made Netflix a $50bn company.** Yahoo's history was
split-adjusted all the way back, so September 2025 closes were ~$120,
multiplied by the ~425M pre-split shares from the 10-Q. Market cap was 10x
too low for ten months, and P/E sat around 5. Yahoo also books some
spinoffs as fractional "splits" (Honeywell 1.061 in Oct 2025, S&P Global
1.057 in Jul 2026), which change the price but not the share count, and
Honeywell's June 2026 event was a 1-for-2 reverse split and a spinoff
rolled into one 0.9535 ratio. `int_stock_splits` works out which part
changes the share count by comparing cover page counts either side.

**Ingestion now records when each price row was fetched.** With daily
incremental loads, a partition written before a split is on a different
basis from one written after. Knowing the fetch time is what makes it
possible to undo Yahoo's adjustment. I checked this by loading the same
year of prices three ways (one backfill, simulated daily fetches, and a
mix with duplicate rows) and getting identical output.

**Share counts: cover page first.** The front page of every 10-Q/10-K has
an actual share count (`dei:EntityCommonStockSharesOutstanding`) dated a
few weeks after quarter end. The model uses whichever of that and the
quarterly count was filed more recently. This also fixed McDonald's,
which files weighted average shares in millions (711.1), which had made
its market cap $223,889.

**But the cover page only lists one share class.** Mastercard's says 122.5M
against ~876M shares actually outstanding, Visa's says 469M against ~1.87B
as-converted, and UPS leaves out Class A. Used blindly those divide market
cap by 7, 4 and 1.2. A cover count is now only used when a us-gaap share
count in the same filing agrees with it within 10%, which is also what
vouches for McDonald's in-millions figure. I checked the surviving counts
against Yahoo's for all 64 tickers: 61 agree within 0.2%, and the three
that don't are exactly MA, V and UPS.

**Companies switch XBRL tags mid-history.** NVDA reported revenue under
`RevenueFromContractWithCustomerExcludingAssessedTax` until January 2022
and has used `Revenues` ever since. My extraction returned on the first
tag that had *any* data, so NVDA got nothing after 2022. Same bug in
mirror image for CAT, which had revenue but no net income, and for PFE,
GOOGL, CVS and GE. The fix reads every candidate tag and merges them.

**Revenue tags overlap in both directions.** When one filing tags two
revenue concepts for the same quarter, a fixed priority is wrong for
someone. ConocoPhillips' `Revenues` ($15.0bn) is the income statement
total and `RevenueFromContractWithCustomer` ($13.3bn) is part of it.
BlackRock's FY2024 10-K has it the other way round. A total can't be
smaller than one of its parts, so the larger value wins. Model revenue
for Q2 2025 now matches the headline figure exactly for all 21 companies I
checked by hand.

**Differencing across a tag switch.** Many filers only report
year-to-date, so standalone quarters are derived (Q2 = H1 − Q1, Q4 =
FY − 9M). MA reported Q1–Q3 2021 under `Revenues` and the full year under
`SalesRevenueNet`, and the difference was -2.589bn of "Q4 revenue". I first
fixed that by never differencing across tags, which quietly lost every Q4
where a company switched to an equivalent tag at year end (NVDA, GOOGL,
AVGO), and a year of TTM with each. Cross-tag quarters are now allowed but
have to land within 0.5–2x of the quarters around them.

**GE's 977% growth wasn't GE.** GE's 10-Ks tag a 91-day `Revenues` fact of
$2.585bn for Q4 2015 and $2.649bn for Q4 2016, against ~$30bn quarters
either side. Whatever line that is, it isn't total revenue, and "as
reported beats derived" trusted it. But around spinoffs it's the derived
number that's wrong: GE's H1 2024 excluded Vernova while its Q1 didn't, so
Q2 came out at $2bn. Now, when the two disagree by more than 20%, whichever
is closer to the neighbouring quarters wins, and a derived quarter wildly
out of line with its neighbours (IBM's $3.3bn Q4 2021 around the Kyndryl
spinoff) is dropped.

**Fiscal quarters aren't always 13 weeks.** Costco and PepsiCo have 16-week
fourth quarters, which fell outside the 80–100 day bucket, and their 24- and
36-week half and nine-month periods fell outside theirs. Neither company
ever got a Q4, so neither ever had a P/E or P/S.

**Holding company reorganizations split history across CIKs.** XOM's
ticker points at ExxonMobil Holdings Corp, which had one 10-Q; Exxon Mobil
Corp still files under the old CIK. BlackRock did the same in 2024. Both
predecessor CIKs are now pulled and merged, and XOM went from no market
cap for 11 months of the dashboard window to full coverage.

**Price partitions could be permanently incomplete.** One failed batch left
2026-08-28 with 16 of 64 tickers, and `--skip-existing` would never
revisit it. It now adds missing tickers to existing dates, and stg_prices
keeps the newest copy when a rewritten file gets loaded twice.

**Banks don't report revenue.** Goldman Sachs has zero revenue rows across
78 quarters. Morgan Stanley has one. What banks, brokers and AmEx report
is revenue net of interest expense, and a price-to-sales ratio on that
means nothing next to Costco's. They're excluded from revenue ratios and
price-to-book is populated. This used to cover all of Financials, which
also blanked P/S for Visa, Mastercard, S&P Global and BlackRock, who report
normal revenue.

## For the dashboard

`mart_sector_daily` has one row per sector per trading day, built for the
sector comparison. It carries cap-weighted P/E, P/S and P/B (total market
cap over the total of the other side, loss makers included) alongside
medians, and `companies_missing_market_cap` so a sector total that's short
a company is visible instead of looking like a valuation story.

Don't average the per-company ratios. `AVG(pe_ratio)` drops every loss
maker and lets one outlier carry the sector: on the last day of the sample
Consumer Discretionary averaged 103 against a median of 21 and a
cap-weighted 29.

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

Every push runs Python linting, an Airflow DAG import check, `dbt parse`,
and two things that actually look at the SQL. None of it needs warehouse
credentials.

`dbt parse` resolves refs and Jinja but never reads the SQL, so a model can
be wrong in every way that matters and still pass. So:

- **`sqlfluff parse`** with the Snowflake dialect, which fails on SQL
  Snowflake can't parse. It won't catch a function that exists in another
  warehouse but not Snowflake, so it's a floor rather than a guarantee.
- **`tests/run_models_duckdb.py`**, which builds every model on DuckDB over
  21 tickers of real SEC and Yahoo data in `tests/fixtures/` (335 KB), runs
  the tests declared in the .yml files and in `dbt/tests/`, and then checks
  values with a known right answer: headline revenue for 10 companies,
  market cap across Netflix's split, which quarter each trading day should
  be using, Visa having no market cap, McDonald's having one.
- **`tests/test_ingestion.py`**, unit tests for the extraction rules. The
  fixtures are what ingestion already produced, so they can't cover
  first-filing-wins, form filtering or the largest-tag rule.

Both suites include cases the current fixture doesn't naturally contain (a
fact tagged years late, a cover count covering one share class, a company
missing a quarter), because otherwise reverting those fixes leaves
everything green. I checked that by reverting each fix one at a time: 16 of
16 broke at least one test.

It is not Snowflake. Where the two dialects differ the SQL is rewritten
before running, so this catches logic and value regressions, not dialect
problems.

Locally:

```bash
pip install duckdb pandas pyarrow pyyaml
python tests/test_ingestion.py
python tests/run_models_duckdb.py
```

A full `dbt build` against Snowflake is a separate manually-triggered
job. It's manual because the warehouse runs on trial credits and a
workflow that starts failing when the trial expires would leave a
permanent red X on the repo.

## Known limitations

**Values are as originally reported.** Ingestion keeps the first filing of
each number, which is what was known at the time. A later restatement is
ignored. Around a spinoff, quarters before and after sit on different
bases, so TTM figures that straddle one mix them (Honeywell's P/E right
after its 2026 separation includes earnings from the business it spun off).

**SEC's companyfacts API is missing some filings.** Citigroup's 2026
10-Qs and S&P Global's FY2024 10-K were filed with XBRL but weren't in the
API in September 2026. Citi's fundamentals go stale (the
`assert_fundamentals_not_stale` test warns) and S&P Global has no TTM for
five months.

**Visa has no market cap.** It only reports share counts per class
(A, B-1, B-2, C), and companyfacts leaves out anything broken down by
class. GOOGL and META have the same problem with cover page counts but
fall back to weighted average shares.

**Splits before the loaded price history aren't known**, so share counts
filed more than 400 days before a trade date aren't used.

**A fetch on a split's ex-date is assumed to be before Yahoo applied it.**
True for the 6am DAG. A manual run later that day could double-adjust;
`assert_no_split_sized_returns` would catch it.

**Net income is as reported, one-offs included.** Alphabet's Q2 2026 10-Q
tags $112.2bn of net income on $119.8bn of revenue, against $40.8bn of
operating income, so about $71bn of it is non-operating. Trailing P/E and
net margin move with that, which is what the filings say but not what the
business earned.

**Sector mapping is hardcoded.** 64 tickers in a `values` list. A real
system would pull SIC codes from SEC's submissions endpoint or use GICS.

**yfinance is unofficial.** It scrapes Yahoo rather than using a licensed
API, so it rate limits and breaks when Yahoo changes their site. There's
retry logic with exponential backoff, but it's a real fragility.

**Weighted-average share counts make market cap approximate** for the
few companies without a cover page count. `shares_basis` tells you which
rows those are.

**Single-node deployment.** LocalExecutor on one EC2 box. Fine for two
DAGs and 64 tickers, not what you'd run for anything real.

## Running it

Needs an AWS account, a Snowflake account, Terraform, Docker and Python.
Copy `.env.example` to `.env` and fill it in first.

```bash
cd terraform && terraform apply    # provision S3, IAM, EC2
```

Then run `snowflake_setup.sql` section by section to create the
warehouse, storage integration, stages and tables. The storage integration
is a two-pass setup: Terraform creates the IAM role, Snowflake generates
the principal and external ID that go in its trust policy, then Terraform
applies again.

```bash
python scripts/build_ticker_universe.py
python scripts/ingest_prices.py
python scripts/ingest_fundamentals.py
```

Run the `COPY INTO` statements (steps 6 and 7), then:

```bash
cd dbt && dbt deps && dbt build
```

Upgrading a warehouse loaded before September 2026: prices gained new
columns and fundamentals changed which filing they keep, so re-pull both
and follow step 9 of `snowflake_setup.sql` to reload.

For the EC2 deployment, clone onto the instance, drop the local AWS
credentials mount from `docker-compose.yaml` (the instance profile
supplies credentials automatically), and `docker compose up -d`.

### Deployment notes

Things that cost me time on the box and aren't obvious from the docs.

**A t3.small can't run this.** 2 GB isn't enough for Airflow 3's four
services plus a task subprocess. It came up fine and then the first
scheduled run hung: the scheduler went unhealthy, tasks failed with
`httpx.ConnectError: Connection refused` trying to reach the API server,
and the scheduler couldn't find the DAG in `serialized_dag`. All of it
traced back to the box sitting at ~300 MB available with half a gig in
swap. t3.medium (4 GB) is the floor, and the Terraform default.

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
per-file logs and silently parses nothing. `airflow dags list` just
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
