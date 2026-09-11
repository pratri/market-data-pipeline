-- One row per company: sector, identifiers and latest reported figures.
--
-- revenue_metric_applicable is there because banks don't report revenue
-- like everyone else. GS has zero revenue rows across 78 quarters, MS has
-- one. Adding bank tags would just produce a P/S for Goldman that can't be
-- compared to Costco's, so revenue ratios are nulled for Financials and
-- P/B is the ratio to use there.

with fundamentals as (

    select * from {{ ref('int_fundamentals_quarterly') }}

),

prices as (

    select * from {{ ref('stg_prices') }}

),

-- Hardcoded for a fixed 64-ticker list. SIC codes from SEC's submissions
-- endpoint would be the proper source.
sectors as (

    select column1 as ticker, column2 as sector
    from values
        ('JPM','Financials'), ('GS','Financials'), ('MS','Financials'),
        ('BAC','Financials'), ('WFC','Financials'), ('C','Financials'),
        ('AXP','Financials'), ('BLK','Financials'), ('SPGI','Financials'),
        ('SCHW','Financials'), ('V','Financials'), ('MA','Financials'),

        ('AAPL','Technology'), ('MSFT','Technology'), ('NVDA','Technology'),
        ('AVGO','Technology'), ('ADBE','Technology'), ('CRM','Technology'),
        ('ORCL','Technology'), ('CSCO','Technology'), ('INTC','Technology'),
        ('AMD','Technology'), ('QCOM','Technology'), ('TXN','Technology'),
        ('IBM','Technology'), ('NOW','Technology'), ('INTU','Technology'),

        ('GOOGL','Communication Services'), ('META','Communication Services'),
        ('DIS','Communication Services'), ('NFLX','Communication Services'),

        ('AMZN','Consumer Discretionary'), ('TSLA','Consumer Discretionary'),
        ('HD','Consumer Discretionary'), ('MCD','Consumer Discretionary'),

        ('COST','Consumer Staples'), ('PEP','Consumer Staples'),
        ('KO','Consumer Staples'), ('WMT','Consumer Staples'),
        ('PG','Consumer Staples'),

        ('UNH','Health Care'), ('JNJ','Health Care'), ('MRK','Health Care'),
        ('ABBV','Health Care'), ('PFE','Health Care'), ('ABT','Health Care'),
        ('TMO','Health Care'), ('DHR','Health Care'), ('BMY','Health Care'),
        ('AMGN','Health Care'), ('GILD','Health Care'), ('CVS','Health Care'),

        ('XOM','Energy'), ('CVX','Energy'), ('COP','Energy'), ('SLB','Energy'),

        ('CAT','Industrials'), ('DE','Industrials'), ('BA','Industrials'),
        ('GE','Industrials'), ('HON','Industrials'), ('UPS','Industrials'),
        ('LMT','Industrials'), ('RTX','Industrials')

),

latest_fundamentals as (

    select
        ticker,
        cik,
        period_end          as latest_period_end,
        filed_date          as latest_filed_date,
        revenue             as latest_quarterly_revenue,
        net_income          as latest_quarterly_net_income,
        total_assets        as latest_total_assets,
        stockholders_equity as latest_stockholders_equity,
        shares_outstanding  as latest_shares_outstanding,
        shares_basis        as latest_shares_basis,
        net_margin          as latest_net_margin,
        revenue_yoy_growth  as latest_revenue_yoy_growth,

        row_number() over (
            partition by ticker order by period_end desc
        ) as rn

    from fundamentals

),

-- actual revenue coverage per company
revenue_coverage as (

    select
        ticker,
        count(*)        as total_quarters,
        count(revenue)  as quarters_with_revenue,
        sum(case when revenue_source = 'derived' then 1 else 0 end)
                        as quarters_derived
    from fundamentals
    group by ticker

),

price_coverage as (

    select
        ticker,
        min(trade_date)  as first_trade_date,
        max(trade_date)  as last_trade_date,
        count(*)         as trading_days,
        max(close_price) as max_close_price,
        min(close_price) as min_close_price
    from prices
    group by ticker

),

latest_close as (

    select
        ticker,
        close_price as latest_close_price,
        row_number() over (partition by ticker order by trade_date desc) as rn
    from prices

)

select
    f.ticker,
    f.cik,
    coalesce(s.sector, 'Unclassified') as sector,

    f.latest_period_end,
    f.latest_filed_date,
    f.latest_quarterly_revenue,
    f.latest_quarterly_net_income,
    f.latest_total_assets,
    f.latest_stockholders_equity,
    f.latest_shares_outstanding,
    f.latest_shares_basis,
    f.latest_net_margin,
    f.latest_revenue_yoy_growth,

    c.latest_close_price,
    c.latest_close_price * f.latest_shares_outstanding as current_market_cap,

    p.first_trade_date,
    p.last_trade_date,
    p.trading_days,
    p.max_close_price,
    p.min_close_price,

    rc.total_quarters,
    rc.quarters_with_revenue,
    rc.quarters_derived,

    -- sector based: no comparable revenue for Financials
    case when coalesce(s.sector, '') = 'Financials' then false else true end
        as revenue_metric_applicable,

    -- coverage based: under 4 quarters of revenue means no TTM, whatever
    -- the sector
    case
        when rc.quarters_with_revenue < 4 then true else false
    end as insufficient_revenue_history,

    -- latest quarter ended more than 6 months ago
    case
        when f.latest_period_end < dateadd('month', -6, current_date())
        then true else false
    end as has_stale_fundamentals

from latest_fundamentals f
left join sectors s          on f.ticker = s.ticker
left join revenue_coverage rc on f.ticker = rc.ticker
left join price_coverage p    on f.ticker = p.ticker
left join latest_close c      on f.ticker = c.ticker and c.rn = 1
where f.rn = 1
