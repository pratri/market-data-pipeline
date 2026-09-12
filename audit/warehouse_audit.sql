-- Warehouse audit queries. Run top to bottom in Snowsight after a build.
-- Schemas assume the profile's ANALYTICS target (dbt appends _STAGING etc).
--
-- "Expect" notes are what a correct build should show, checked by running
-- the models in DuckDB against SEC and Yahoo data pulled in Sept 2026.
-- "Was" notes are what the old build showed, for comparison.

use database MARKET_DATA;


-- ===========================================================================
-- 1. Is each trading day using the right quarter?
-- ===========================================================================

-- 1a. Age of the attached quarter by month. A large cap's latest quarter
-- shouldn't be much more than ~150 days old.
-- Expect: max age under ~150 most months; C pushes Jul-Sep 2026 past 200
-- because SEC's API is missing its 2026 10-Qs.
-- Was: 87% of Sep 2025 rows over 200 days, with normal days_since_filing.
select
    date_trunc('month', trade_date)                                        as month,
    count(*)                                                               as row_count,
    round(avg(datediff('day', fundamentals_period_end, trade_date)))       as avg_quarter_age_days,
    max(datediff('day', fundamentals_period_end, trade_date))              as max_quarter_age_days,
    round(avg(iff(datediff('day', fundamentals_period_end, trade_date) > 200, 1, 0)), 3)
                                                                           as share_over_200d,
    round(avg(days_since_filing))                                          as avg_days_since_filing
from ANALYTICS_MARTS.FCT_DAILY_METRICS
group by 1
order by 1;

-- Which tickers are stale, and since when.
select ticker, min(trade_date) as stale_from, max(fundamentals_period_end) as last_quarter
from ANALYTICS_MARTS.FCT_DAILY_METRICS
where datediff('day', fundamentals_period_end, trade_date) > 200
group by 1
order by 2;

-- 1b. AAPL, one row per change in attached quarter.
-- Expect: 2025-09-02 -> 2025-06-28 ($94.0bn), 2025-10-31 -> 2025-09-27,
--         2026-01-30 -> 2025-12-27, 2026-05-01 -> 2026-03-28.
-- Was: 2025-09-11 -> 2024-06-29 ($85.8bn), 2025-10-31 -> 2023-09-30.
select trade_date, fundamentals_period_end, fundamentals_filed_date,
       revenue, ttm_net_income, pe_ratio, market_cap
from ANALYTICS_MARTS.FCT_DAILY_METRICS
where ticker = 'AAPL'
qualify fundamentals_period_end is distinct from
        lag(fundamentals_period_end) over (order by trade_date)
order by trade_date;

-- 1c. Filed dates should be the first filing. Large filers have 40 days
-- for a 10-Q and 60 for a 10-K.
-- Expect: roughly 6% of facts over 150 days (late-tagged history, amended
-- filings, SEC gaps). Was: ~70%.
select period_type,
       count(*) as facts,
       round(avg(iff(datediff('day', period_end, filed) > 150, 1, 0)), 3) as share_filed_late
from ANALYTICS_STAGING.STG_FUNDAMENTALS
group by 1
order by 1;

-- 1d. fiscal_year should match the period for calendar-year filers.
-- Expect: nearly all 0.
select fiscal_year - year(period_end) as fy_minus_calendar_year, count(*)
from ANALYTICS_INTERMEDIATE.INT_FUNDAMENTALS_QUARTERLY
where ticker in ('JPM', 'BAC', 'KO', 'PFE', 'CVX', 'IBM', 'GOOGL', 'META', 'AMZN', 'TSLA')
group by 1
order by 1;


-- ===========================================================================
-- 2. Coverage holes that skew sector aggregates
-- ===========================================================================

-- 2a. Tickers missing market cap, P/E or P/S on many days.
-- Expect: V (no share count in SEC data), INTC and BA (losses, so no P/E),
-- SPGI (no TTM Aug 2025-Feb 2026, SEC's API lacks its FY2024 10-K), and the
-- banks with no P/S by design.
select ticker, sector,
       count(*) as days,
       count(market_cap) as days_mcap,
       count(pe_ratio) as days_pe,
       count(price_to_sales) as days_ps,
       count(price_to_book) as days_pb,
       count_if(ttm_net_income < 0) as days_negative_ttm
from ANALYTICS_MARTS.FCT_DAILY_METRICS
group by 1, 2
having count(pe_ratio) < count(*) * 0.9
    or count(price_to_sales) < count(*) * 0.9
    or count(market_cap) < count(*)
order by days_mcap, days_pe;

-- 2b. Durations that fit no bucket. Expect only a few GS and MS oddities.
select ticker, metric, period_days, count(*) as n
from ANALYTICS_STAGING.STG_FUNDAMENTALS
where period_type = 'other'
  and metric in ('revenue', 'net_income')
group by 1, 2, 3
order by n desc;

-- 2c. Quarters with no revenue sandwiched between two that have it.
-- Expect: IBM 2021 Q4 (dropped on purpose, Kyndryl), SPGI 2024 Q4 (SEC gap),
-- and a few pre-2020 cases.
select ticker, period_end, prev_revenue, next_revenue
from (
    select ticker, period_end, revenue,
           lag(revenue)  over (partition by ticker order by period_end) as prev_revenue,
           lead(revenue) over (partition by ticker order by period_end) as next_revenue
    from ANALYTICS_INTERMEDIATE.INT_FUNDAMENTALS_QUARTERLY
)
where revenue is null
  and prev_revenue is not null
  and next_revenue is not null
order by period_end desc;

-- 2d. Sector coverage changing day to day. Any row is a step in a sector
-- time series caused by data rather than the market.
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

-- 2e. Partial price days. Expect none.
select trade_date, count(distinct ticker) as tickers
from ANALYTICS_STAGING.STG_PRICES
group by 1
having count(distinct ticker) < 64
order by 1;


-- ===========================================================================
-- 3. Market cap
-- ===========================================================================

-- 3a. Around NFLX's 10-for-1. Expect close ~$1,112 then ~$110 with market cap
-- steady at ~$470bn. Was: ~$52bn in Sep 2025.
select trade_date, close_price, split_adjusted_close_price, shares_outstanding,
       shares_basis, round(market_cap / 1e9, 1) as mcap_bn
from ANALYTICS_MARTS.FCT_DAILY_METRICS
where ticker = 'NFLX'
  and trade_date between '2025-11-10' and '2025-11-21'
order by trade_date;

-- 3b. Every price adjustment and what it did to the share count.
-- Expect: NFLX 10 -> 10, NOW 5 -> 5, HON 1.061 -> null (spinoff),
-- HON 0.9535 -> 0.5 (reverse split + spinoff), SPGI 1.057 -> null.
select * from ANALYTICS_INTERMEDIATE.INT_STOCK_SPLITS order by split_date;

-- 3c. Latest market caps, smallest first. Everything here is > $50bn, so
-- anything tiny or null needs a reason. Expect V null.
select ticker, sector, close_price, round(market_cap / 1e9, 1) as mcap_bn,
       shares_outstanding, shares_basis, days_since_filing
from ANALYTICS_MARTS.FCT_DAILY_METRICS
where trade_date = (select max(trade_date) from ANALYTICS_MARTS.FCT_DAILY_METRICS)
order by market_cap nulls first;

-- 3c-2. Cover page counts that were rejected because the same filing's
-- us-gaap count disagrees. Expect MA (one share class on the cover), UPS
-- (Class A left out) and V (no us-gaap count at all, so nothing to check).
with cover as (
    select ticker, filed, period_end, value as cover_shares
    from ANALYTICS_STAGING.STG_FUNDAMENTALS
    where metric = 'cover_shares_outstanding' and value >= 1000000
),
filing_counts as (
    select ticker, filed, min(value) as min_count, max(value) as max_count
    from ANALYTICS_STAGING.STG_FUNDAMENTALS
    where metric = 'shares_outstanding' and value > 0
    group by 1, 2
)
select c.ticker, c.filed, c.cover_shares, f.min_count, f.max_count,
       round(c.cover_shares / nullif(f.max_count, 0), 3) as cover_over_max
from cover c
left join filing_counts f on f.ticker = c.ticker and f.filed = c.filed
where c.filed >= dateadd('month', -18, current_date())
  and (f.ticker is null
       or least(abs(c.cover_shares / f.max_count - 1),
                abs(c.cover_shares / (f.max_count * 1000000) - 1)) > 0.1)
order by c.ticker, c.filed;

-- 3d. Share count jumps between quarters (as filed, so splits show up here
-- on purpose). Anything else outside 0.8-1.25x is worth a look.
select ticker, period_end, shares_basis, shares_outstanding, prev_shares,
       round(shares_outstanding / prev_shares, 3) as ratio
from (
    select ticker, period_end, shares_basis, shares_outstanding,
           lag(shares_outstanding) over (partition by ticker order by period_end) as prev_shares
    from ANALYTICS_INTERMEDIATE.INT_FUNDAMENTALS_QUARTERLY
)
where prev_shares > 0
  and period_end >= '2024-01-01'
  and shares_outstanding / prev_shares not between 0.8 and 1.25
order by period_end desc;


-- ===========================================================================
-- 4. Revenue quality
-- ===========================================================================

-- 4a. Four quarters vs the reported annual figure. Differences of a few
-- percent are expected around spinoffs (quarters as originally reported,
-- FY restated to continuing operations: GE 2024, DHR 2023, JNJ 2023).
with fy as (
    select ticker, period_start, period_end, tag, value
    from ANALYTICS_STAGING.STG_FUNDAMENTALS
    where metric = 'revenue' and period_type = 'annual'
)
select fy.ticker, fy.period_end, fy.tag,
       round(fy.value / 1e9, 2)                as fy_bn,
       round(sum(q.revenue) / 1e9, 2)          as quarters_bn,
       count(q.revenue)                        as n_quarters,
       round(sum(q.revenue) / fy.value - 1, 3) as diff
from fy
join ANALYTICS_INTERMEDIATE.INT_FUNDAMENTALS_QUARTERLY q
    on  q.ticker = fy.ticker
    and q.period_end > fy.period_start
    and q.period_end <= fy.period_end
group by 1, 2, 3, 4, fy.value
having count(q.revenue) = 4
   and abs(sum(q.revenue) / fy.value - 1) > 0.02
order by fy.period_end desc;

-- 4b. Big QoQ moves with the source of both sides. Expect only real ones
-- (TSLA 2012, Intuit's tax-season quarter, 2008-09 banks).
select ticker, period_end, revenue, revenue_source,
       prev_revenue, prev_source, revenue_qoq_growth
from (
    select *,
           lag(revenue)        over (partition by ticker order by period_end) as prev_revenue,
           lag(revenue_source) over (partition by ticker order by period_end) as prev_source
    from ANALYTICS_INTERMEDIATE.INT_FUNDAMENTALS_QUARTERLY
)
where abs(revenue_qoq_growth) > 1
order by abs(revenue_qoq_growth) desc;

-- 4c. GE around 2015-2017. Expect ~$25-34bn every quarter, no 977%.
select period_end, round(revenue / 1e9, 2) as revenue_bn, revenue_source,
       revenue_qoq_growth, filed_date
from ANALYTICS_INTERMEDIATE.INT_FUNDAMENTALS_QUARTERLY
where ticker = 'GE'
  and period_end between '2015-01-01' and '2018-06-30'
order by period_end;

-- 4d. Spot check against headline revenue for Q2 2025 ($bn). Expect exact
-- matches: AAPL 94.036, MSFT 76.441, GOOGL 96.428, AMZN 167.702, META 47.516,
-- TSLA 22.496, NVDA 46.743 (Jul 27), CAT 16.569, WMT 177.402 (Jul 31),
-- JNJ 23.743, XOM 81.506, V 10.172, MA 8.133.
select ticker, period_end, round(revenue / 1e9, 3) as revenue_bn, revenue_source
from ANALYTICS_INTERMEDIATE.INT_FUNDAMENTALS_QUARTERLY
where ticker in ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA',
                 'CAT', 'WMT', 'JNJ', 'XOM', 'V', 'MA')
  and period_end between '2025-06-01' and '2025-07-31'
order by ticker;


-- ===========================================================================
-- 5. Tableau: sector aggregates
-- ===========================================================================

-- mart_sector_daily is built for this. Cap-weighted ratios include loss
-- makers, medians show the typical company, and companies_missing_market_cap
-- says whether the total is short anyone (V has no share count).
select sector, companies, companies_missing_market_cap, companies_with_pe,
       round(total_market_cap / 1e9)   as mcap_bn,
       round(cap_weighted_pe, 1)       as cap_weighted_pe,
       round(median_pe, 1)             as median_pe,
       round(cap_weighted_ps, 2)       as cap_weighted_ps,
       round(cap_weighted_pb, 1)       as cap_weighted_pb,
       round(median_revenue_yoy_growth, 3) as median_yoy_growth
from ANALYTICS_MARTS.MART_SECTOR_DAILY
where trade_date = (select max(trade_date) from ANALYTICS_MARTS.MART_SECTOR_DAILY)
order by mcap_bn desc;

-- What averaging the ratios instead would tell you. Expect the average to
-- sit well above both the median and the cap-weighted figure for at least
-- one sector.
select sector,
       round(avg(pe_ratio), 1)    as avg_pe,
       round(median(pe_ratio), 1) as median_pe,
       count(pe_ratio)            as with_pe,
       count(*)                   as companies
from ANALYTICS_MARTS.FCT_DAILY_METRICS
where trade_date = (select max(trade_date) from ANALYTICS_MARTS.FCT_DAILY_METRICS)
group by 1
order by avg_pe desc;

-- Sector coverage over time. Every change should have a reason (a loss
-- quarter entering TTM, a company's first filing).
select trade_date, sector, companies_with_market_cap, companies_with_pe
from ANALYTICS_MARTS.MART_SECTOR_DAILY
qualify companies_with_market_cap is distinct from
            lag(companies_with_market_cap) over (partition by sector order by trade_date)
     or companies_with_pe is distinct from
            lag(companies_with_pe) over (partition by sector order by trade_date)
order by trade_date;


-- ===========================================================================
-- 6. Load hygiene
-- ===========================================================================

-- Snapshot completeness. Each ticker reads its own newest snapshot, so a
-- short snapshot doesn't drop anyone, but it's worth knowing about.
select regexp_substr(source_file, 'snapshot_date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1, 1, 'e', 1) as snapshot,
       count(distinct ticker) as tickers,
       count(*) as row_count,
       count(distinct loaded_at) as load_batches
from RAW.FUNDAMENTALS
group by 1
order by 1 desc;

-- Rows loaded more than once (rewritten partitions). stg_prices keeps the
-- newest; this is just to see how often it happens.
select trade_date, ticker, count(*) as copies, max(fetched_at) as newest_fetch
from RAW.PRICES
group by 1, 2
having count(*) > 1
order by 1 desc;

-- Price rows with no fetch time can't be put back to as-traded prices.
-- Expect 0 after the step 9 reload.
select count(*) as rows_without_fetched_at
from RAW.PRICES
where fetched_at is null;
