-- One row per sector per trading day, for the sector comparison dashboard.
--
-- Aggregate ratios rather than averages of ratios. AVG(pe_ratio) drops every
-- loss-making company and lets one outlier carry the sector: Consumer
-- Discretionary averaged 103 against a median of 22. Cap-weighted P/E is
-- total market cap over total TTM earnings, loss makers included, which is
-- what a sector multiple actually means. Medians are there for the
-- "typical company" view.
--
-- companies_missing_market_cap keeps the gaps visible. A sector total that
-- silently leaves someone out (Visa has no share count in SEC's data) looks
-- like a valuation story instead of a coverage problem.

with daily as (

    select * from {{ ref('fct_daily_metrics') }}

)

select
    trade_date,
    sector,

    count(*)                        as companies,
    count(market_cap)               as companies_with_market_cap,
    count(*) - count(market_cap)    as companies_missing_market_cap,
    count(pe_ratio)                 as companies_with_pe,
    count(price_to_sales)           as companies_with_ps,

    sum(market_cap)                 as total_market_cap,
    sum(ttm_net_income)             as total_ttm_net_income,
    sum(ttm_revenue)                as total_ttm_revenue,

    -- cap-weighted: only companies that have both sides of the ratio
    sum(case when ttm_net_income is not null then market_cap end)
        / nullif(sum(case when market_cap is not null then ttm_net_income end), 0)
                                    as cap_weighted_pe,

    sum(case when ttm_revenue is not null then market_cap end)
        / nullif(sum(case when market_cap is not null then ttm_revenue end), 0)
                                    as cap_weighted_ps,

    sum(case when stockholders_equity > 0 then market_cap end)
        / nullif(sum(case when market_cap is not null and stockholders_equity > 0
                          then stockholders_equity end), 0)
                                    as cap_weighted_pb,

    median(pe_ratio)                as median_pe,
    median(price_to_sales)          as median_ps,
    median(price_to_book)           as median_pb,
    median(net_margin)              as median_net_margin,
    median(revenue_yoy_growth)      as median_revenue_yoy_growth,
    median(annualized_volatility_20d) as median_volatility

from daily
group by trade_date, sector
