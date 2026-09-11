-- Warehouse audit queries. Run top to bottom in Snowsight.
-- Schemas assume the profile's ANALYTICS target (dbt appends _STAGING etc).
-- "Expect" notes come from replaying the models on the local Aug 28 parquet
-- and the tableau/market_metrics.csv export.

use database MARKET_DATA;


-- ===========================================================================
-- 1. Fundamentals attached to the wrong quarter (as-of join)
-- ===========================================================================

-- 1a. Age of the attached quarter by month. A large-cap's latest quarter
-- should never be much more than ~150 days old. days_since_filing will look
-- normal because filed_date is wrong too.
-- Expect: ~87% of rows over 200 days in Sep 2025, ~60% in Apr 2026.
select
    date_trunc('month', trade_date)                                        as month,
    count(*)                                                               as row_count,
    round(avg(datediff('day', fundamentals_period_end, trade_date)))       as avg_quarter_age_days,
    round(avg(iff(datediff('day', fundamentals_period_end, trade_date) > 200, 1, 0)), 2)
                                                                           as share_over_200d,
    round(avg(days_since_filing))                                          as avg_days_since_filing
from ANALYTICS_MARTS.FCT_DAILY_METRICS
group by 1
order by 1;

-- 1b. AAPL, one row per change in attached quarter.
-- Expect: 2025-09-11 -> 2024-06-29 (rev 85.777bn, a year old),
--         2025-10-31 -> 2023-09-30 (two years old),
--         2026-01-30 -> no change even though Q1 FY26 was filed that day.
select trade_date, fundamentals_period_end, fundamentals_filed_date,
       revenue, ttm_net_income, pe_ratio
from ANALYTICS_MARTS.FCT_DAILY_METRICS
where ticker = 'AAPL'
qualify fundamentals_period_end is distinct from
        lag(fundamentals_period_end) over (order by trade_date)
order by trade_date;

-- 1c. Root cause. dedupe keeps the LAST filing that mentioned a period, and
-- every 10-Q/10-K repeats prior periods as comparatives. So "filed" is when
-- the period was last repeated, not when it was first public.
-- Expect: AAPL Q3 FY24 (ends 2024-06-29) filed 2025-08-01, fiscal_year 2025.
select metric, period_start, period_end, period_type, value, fiscal_year,
       fiscal_period, form, filed,
       datediff('day', period_end, filed) as days_after_period_end
from ANALYTICS_STAGING.STG_FUNDAMENTALS
where ticker = 'AAPL'
  and metric = 'revenue'
  and period_end >= '2024-01-01'
order by period_end, period_start;

-- 1d. How widespread. Large accelerated filers have 40 days for a 10-Q and
-- 60 for a 10-K, so anything past 150 is a re-report.
-- Expect: roughly 70% of all facts.
select period_type,
       count(*) as facts,
       round(avg(iff(datediff('day', period_end, filed) > 150, 1, 0)), 3) as share_filed_late
from ANALYTICS_STAGING.STG_FUNDAMENTALS
group by 1
order by 1;

-- 1e. fiscal_year/fiscal_period come from the filing, not the period.
-- For these calendar-year filers the difference should always be 0.
select fiscal_year - year(period_end) as fy_minus_calendar_year, count(*)
from ANALYTICS_INTERMEDIATE.INT_FUNDAMENTALS_QUARTERLY
where ticker in ('JPM', 'BAC', 'KO', 'PFE', 'CVX', 'IBM', 'GOOGL', 'META', 'AMZN', 'TSLA')
group by 1
order by 1;


-- ===========================================================================
-- 2. Derived Q4 dropped by the 200-day filing-gap guard
-- ===========================================================================

-- Because of 1c, FY(Y) is "filed" with the 10-K two years later while 9M(Y)
-- is "filed" with next year's Q3 10-Q. That's ~15 months apart for almost
-- every company, not just IBM, so the guard removes nearly every older Q4.
-- Expect: annual rows rejected ~100% for years before the last two.
with s as (
    select
        ticker, metric, tag, period_start, period_end, period_type, filed,
        lag(filed) over (partition by ticker, metric, period_start, tag order by period_end)      as prev_filed,
        lag(period_end) over (partition by ticker, metric, period_start, tag order by period_end) as prev_end
    from ANALYTICS_STAGING.STG_FUNDAMENTALS
    where metric = 'revenue'
      and period_type in ('quarterly', 'half_year', 'nine_month', 'annual')
)
select year(period_end) as yr,
       period_type,
       count(*) as diff_candidates,
       count_if(datediff('day', prev_filed, filed) not between 0 and 200) as rejected_by_filed_gap
from s
where prev_filed is not null
  and datediff('day', prev_end, period_end) between 80 and 100
group by 1, 2
order by 1, 2;

-- Revenue coverage per year for non-financials. A year with 4 quarter rows
-- and only 3 revenues is a missing Q4.
select year(i.period_end) as yr,
       count(*) as quarter_rows,
       count(i.revenue) as with_revenue,
       count_if(i.revenue_source = 'derived') as derived
from ANALYTICS_INTERMEDIATE.INT_FUNDAMENTALS_QUARTERLY i
join ANALYTICS_MARTS.DIM_COMPANIES d using (ticker)
where d.revenue_metric_applicable
group by 1
order by 1;


-- ===========================================================================
-- 3. Coverage holes that skew sector aggregates
-- ===========================================================================

-- 3a. Tickers with no P/E, P/S or market cap for the whole window.
-- Expect: V (no market cap at all), XOM (no fundamentals until Aug 2026),
-- COST, PEP, CAT, MA, PG, UNH with zero P/E despite being profitable.
select ticker, sector,
       count(*) as days,
       count(market_cap) as days_mcap,
       count(pe_ratio) as days_pe,
       count(price_to_sales) as days_ps,
       count(price_to_book) as days_pb
from ANALYTICS_MARTS.FCT_DAILY_METRICS
group by 1, 2
having count(pe_ratio) < count(*) * 0.5 or count(market_cap) < count(*)
order by days_mcap, days_pe;

-- 3b. 52/53-week filers on a 12/12/12/16-week calendar (COST, PEP).
-- Q4 (111-118 days), H1 (167) and 9M (251) all fall outside the buckets,
-- so there's never a Q4 and TTM never gets 4 quarters.
select ticker, metric, period_days, count(*) as n
from ANALYTICS_STAGING.STG_FUNDAMENTALS
where period_type = 'other'
  and metric in ('revenue', 'net_income')
group by 1, 2, 3
order by n desc;

-- 3c. Net income only from proxy statements. CAT and MA have no quarterly
-- net income after 2011/2014 in the local snapshot, only annual DEF 14A
-- (pay-versus-performance) rows.
select ticker, form, period_type,
       min(period_end) as first_period, max(period_end) as last_period, count(*) as n
from ANALYTICS_STAGING.STG_FUNDAMENTALS
where metric = 'net_income'
  and ticker in ('CAT', 'MA', 'PG', 'UNH')
group by all
order by ticker, last_period;

-- Non 10-K/10-Q forms that won the dedupe (8-K recasts, proxy statements).
select form, metric, count(*) as n, count(distinct ticker) as tickers
from ANALYTICS_STAGING.STG_FUNDAMENTALS
where form not in ('10-K', '10-Q', '10-K/A', '10-Q/A')
group by 1, 2
order by n desc;

-- 3d. Share count coverage. V has zero non-dimensional share rows (it only
-- tags per class), XOM's history starts at its new holdco CIK.
select d.ticker, d.cik,
       min(s.period_end) as first_shares_period,
       min(s.filed)      as first_shares_filed,
       count(s.value)    as share_rows
from ANALYTICS_MARTS.DIM_COMPANIES d
left join ANALYTICS_STAGING.STG_FUNDAMENTALS s
    on s.ticker = d.ticker and s.metric = 'shares_outstanding'
group by 1, 2
order by share_rows, first_shares_filed desc;

-- 3e. Sector coverage changing day to day. Any row here is a step in a
-- sector time series caused by data, not the market.
-- Expect: Energy jumps when XOM appears (Aug 2026), 2026-08-28 drops to 16
-- tickers total.
with d as (
    select trade_date, sector,
           count(*) as tickers,
           count(market_cap) as with_mcap,
           count(pe_ratio) as with_pe,
           sum(market_cap) as sector_mcap
    from ANALYTICS_MARTS.FCT_DAILY_METRICS
    group by 1, 2
)
select *,
       with_mcap - lag(with_mcap) over (partition by sector order by trade_date) as mcap_coverage_change,
       with_pe - lag(with_pe) over (partition by sector order by trade_date)     as pe_coverage_change,
       round(sector_mcap / lag(sector_mcap) over (partition by sector order by trade_date) - 1, 3)
                                                                                 as sector_mcap_change
from d
qualify mcap_coverage_change != 0
     or pe_coverage_change != 0
     or abs(sector_mcap_change) > 0.05
order by trade_date, sector;

-- 3f. Partial price partitions. --skip-existing never refills these.
select trade_date, count(distinct ticker) as tickers
from ANALYTICS_STAGING.STG_PRICES
group by 1
having count(distinct ticker) < 64
order by 1;


-- ===========================================================================
-- 4. Market cap wrong by a constant factor
-- ===========================================================================

-- 4a. NFLX 10-for-1 split (Nov 2025). Yahoo prices are split-adjusted back
-- to the start, share counts are as filed.
-- Expect: ~$52bn in Sep 2025 (real ~$500bn), jumps to ~$287bn on 2026-07-17.
select trade_date, close_price, shares_outstanding, shares_basis,
       fundamentals_period_end, round(market_cap / 1e9, 1) as mcap_bn
from ANALYTICS_MARTS.FCT_DAILY_METRICS
where ticker = 'NFLX'
qualify shares_outstanding is distinct from lag(shares_outstanding) over (order by trade_date)
order by trade_date;

-- 4b. Market cap moving differently from price. Catches splits, unit
-- errors (MCD), share basis flips and quarter swaps.
select ticker, trade_date, daily_return,
       market_cap / lag(market_cap) over (partition by ticker order by trade_date) - 1 as mcap_change,
       shares_basis
from ANALYTICS_MARTS.FCT_DAILY_METRICS
qualify abs(mcap_change - daily_return) > 0.05
order by abs(mcap_change - daily_return) desc;

-- 4c. Share count jumps between quarters. Real buybacks move a few percent
-- a year; anything outside 0.8-1.25x is a split, a unit problem, or
-- instant vs weighted-average flipping.
select ticker, period_end, shares_basis, shares_outstanding, prev_shares,
       round(shares_outstanding / prev_shares, 3) as ratio
from (
    select ticker, period_end, shares_basis, shares_outstanding,
           lag(shares_outstanding) over (partition by ticker order by period_end) as prev_shares
    from ANALYTICS_INTERMEDIATE.INT_FUNDAMENTALS_QUARTERLY
)
where prev_shares > 0
  and shares_outstanding / prev_shares not between 0.8 and 1.25
order by abs(ln(shares_outstanding / prev_shares)) desc;

-- 4d. Latest market caps, smallest first. Every company here is > $50bn,
-- so anything tiny or null is wrong. Check MCD after the <1M null fix: it
-- may now have no market cap at all.
select ticker, sector, round(market_cap / 1e9, 1) as mcap_bn,
       shares_outstanding, shares_basis, days_since_filing
from ANALYTICS_MARTS.FCT_DAILY_METRICS
where trade_date = (select max(trade_date) from ANALYTICS_MARTS.FCT_DAILY_METRICS)
order by market_cap nulls first;


-- ===========================================================================
-- 5. TTM and ratio logic
-- ===========================================================================

-- 5a. Uncommitted change: P/S now checks income_quarters_in_ttm instead of
-- revenue_quarters_in_ttm, so a TTM with a missing revenue quarter (sum
-- skips nulls) gets through and P/S is inflated. Should return 0.
select count(*) as ps_without_full_revenue_ttm
from ANALYTICS_MARTS.FCT_DAILY_METRICS
where price_to_sales is not null
  and ttm_revenue is null;

-- 5b. P/E has no span check. Four net-income rows spanning far more than a
-- year still produce a P/E.
with t as (
    select ticker, period_end,
           count(net_income) over (partition by ticker order by period_end rows between 3 preceding and current row) as n_q,
           datediff('day',
               min(period_end) over (partition by ticker order by period_end rows between 3 preceding and current row),
               period_end) as span_days
    from ANALYTICS_INTERMEDIATE.INT_FUNDAMENTALS_QUARTERLY
)
select t.ticker, t.period_end, t.span_days, count(*) as fct_days_using_it
from t
join ANALYTICS_MARTS.FCT_DAILY_METRICS f
    on f.ticker = t.ticker and f.fundamentals_period_end = t.period_end
where t.n_q = 4
  and t.span_days not between 250 and 290
  and f.pe_ratio is not null
group by 1, 2, 3
order by span_days desc;

-- 5c. Same quarter split across two period_end dates a few days apart.
-- Breaks the 4-row TTM window and lag(x, 4).
select ticker, prev_end, period_end,
       datediff('day', prev_end, period_end) as gap_days,
       revenue, net_income, total_assets, shares_outstanding
from (
    select *, lag(period_end) over (partition by ticker order by period_end) as prev_end
    from ANALYTICS_INTERMEDIATE.INT_FUNDAMENTALS_QUARTERLY
)
where datediff('day', prev_end, period_end) < 20
order by ticker, period_end;

-- 5d. Four quarters vs the reported annual figure. Mismatches mean the
-- quarters mix tags or bases (as-reported Q1-Q3 vs a restated FY).
with fy as (
    select ticker, period_start, period_end, tag, value
    from ANALYTICS_STAGING.STG_FUNDAMENTALS
    where metric = 'revenue' and period_type = 'annual'
)
select fy.ticker, fy.period_end, fy.tag,
       round(fy.value / 1e9, 2)          as fy_bn,
       round(sum(q.revenue) / 1e9, 2)    as quarters_bn,
       count(q.revenue)                  as n_quarters,
       round(sum(q.revenue) / fy.value - 1, 3) as diff
from fy
join ANALYTICS_INTERMEDIATE.INT_FUNDAMENTALS_QUARTERLY q
    on  q.ticker = fy.ticker
    and q.period_end > fy.period_start
    and q.period_end <= fy.period_end
group by 1, 2, 3, 4, fy.value
having count(q.revenue) = 4
   and abs(sum(q.revenue) / fy.value - 1) > 0.02
order by abs(diff) desc;

-- 5e. Tag switches between consecutive as-reported quarters, with the value
-- jump. Big jumps at a tag change are scope changes, not growth.
select ticker, period_end, prev_tag, tag, prev_value, value,
       round(value / nullif(prev_value, 0) - 1, 3) as change
from (
    select ticker, period_end, tag, value,
           lag(tag)   over (partition by ticker order by period_end) as prev_tag,
           lag(value) over (partition by ticker order by period_end) as prev_value
    from ANALYTICS_STAGING.STG_FUNDAMENTALS
    where metric = 'revenue' and period_type = 'quarterly'
)
where tag != prev_tag
order by abs(change) desc nulls last;


-- ===========================================================================
-- 6. GE and other QoQ outliers
-- ===========================================================================

-- 6a. Every big QoQ move with the source of BOTH sides. revenue_source only
-- describes the numerator. If outliers cluster where the previous quarter is
-- a derived fiscal Q4, it's the derivation, not the company.
select ticker, period_end, revenue, revenue_source,
       prev_revenue, prev_source, prev_period_end, revenue_qoq_growth
from (
    select *,
           lag(revenue)        over (partition by ticker order by period_end) as prev_revenue,
           lag(revenue_source) over (partition by ticker order by period_end) as prev_source,
           lag(period_end)     over (partition by ticker order by period_end) as prev_period_end
    from ANALYTICS_INTERMEDIATE.INT_FUNDAMENTALS_QUARTERLY
)
where abs(revenue_qoq_growth) > 1
order by abs(revenue_qoq_growth) desc;

-- 6b. GE raw facts around the 2015-2017 boundaries: which tag, which
-- duration and which filing each side came from.
select metric, tag, period_start, period_end, period_type,
       round(value / 1e9, 2) as bn, form, filed
from ANALYTICS_STAGING.STG_FUNDAMENTALS
where ticker = 'GE'
  and metric = 'revenue'
  and period_end between '2015-01-01' and '2017-12-31'
order by period_end, period_start;

-- 6c. What the model made of it.
select period_end, round(revenue / 1e9, 2) as revenue_bn, revenue_source,
       round(lag(revenue) over (order by period_end) / 1e9, 2) as prev_bn,
       lag(revenue_source) over (order by period_end) as prev_source,
       revenue_qoq_growth, filed_date
from ANALYTICS_INTERMEDIATE.INT_FUNDAMENTALS_QUARTERLY
where ticker = 'GE'
  and period_end between '2015-01-01' and '2018-06-30'
order by period_end;


-- ===========================================================================
-- 7. Tableau: sector aggregates
-- ===========================================================================

-- AVG(pe_ratio) drops loss-makers and lets one outlier dominate
-- (Consumer Discretionary: mean 103 vs median 22 on 2026-08-27; Financials
-- P/B max 93). Compare against cap-weighted.
select sector,
       count(*)                                  as tickers,
       count(pe_ratio)                           as with_pe,
       round(avg(pe_ratio), 1)                   as avg_pe,
       round(median(pe_ratio), 1)                as median_pe,
       round(sum(iff(ttm_net_income is not null, market_cap, null))
             / nullif(sum(ttm_net_income), 0), 1) as cap_weighted_pe,
       round(median(price_to_book), 1)           as median_pb,
       round(max(price_to_book), 1)              as max_pb,
       round(sum(market_cap) / 1e9)              as sector_mcap_bn,
       count(market_cap)                         as with_mcap
from ANALYTICS_MARTS.FCT_DAILY_METRICS
where trade_date = (select max(trade_date) from ANALYTICS_MARTS.FCT_DAILY_METRICS)
group by 1
order by sector_mcap_bn desc;


-- ===========================================================================
-- 8. Load hygiene
-- ===========================================================================

-- Snapshot completeness. A --limit run or a bad SEC day becomes "latest".
select regexp_substr(source_file, 'snapshot_date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1, 1, 'e', 1) as snapshot,
       count(distinct ticker) as tickers,
       count(*) as row_count,
       count(distinct loaded_at) as load_batches
from RAW.FUNDAMENTALS
group by 1
order by 1 desc;

-- Re-uploaded files get loaded again by COPY INTO (new checksum).
select trade_date, ticker, count(*) as copies
from RAW.PRICES
group by 1, 2
having count(*) > 1
order by 1 desc;
