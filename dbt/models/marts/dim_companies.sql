-- One row per company: identifiers, sector, and a snapshot of the most
-- recent reported figures.
--
-- The revenue_metric_applicable flag exists because of a real finding,
-- not a convenience. Goldman Sachs has zero revenue rows across 78
-- quarters; Morgan Stanley has one. That isn't a tag-list gap, it's
-- that banks don't report "revenue" as a concept. They report interest
-- income, non-interest income, and revenues net of interest expense,
-- which measure something structurally different from a retailer's
-- product sales.
--
-- The wrong fix is adding bank tags so the column fills up: you'd get a
-- price-to-sales ratio for Goldman that looks valid and is meaningless,
-- because it isn't comparable to the same ratio for Costco. Analysts
-- don't value banks on P/S; they use price-to-book, which this model
-- computes and which works correctly for financials.
--
-- So the flag is set explicitly and revenue-based ratios are suppressed
-- for these companies. A visible null with a documented reason beats an
-- invisible wrong number.

with fundamentals as (

    select * from {{ ref('int_fundamentals_quarterly') }}

),

prices as (

    select * from {{ ref('stg_prices') }}

),

-- Hardcoded sector mapping. A production system would source this from
-- a reference dataset (GICS, or SEC's own SIC codes, which are in the
-- submissions endpoint). Hardcoding a fixed 64-ticker universe is
-- honest about scope rather than pretending to a classification
-- pipeline that doesn't exist here.
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

-- Actual coverage per company, so the flag reflects the data rather
-- than only the sector assumption.
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

    -- Sector-based: banks and capital-markets firms don't report a
    -- comparable revenue figure under US-GAAP XBRL.
    case when coalesce(s.sector, '') = 'Financials' then false else true end
        as revenue_metric_applicable,

    -- Coverage-based: catches any other company whose revenue is too
    -- sparse to compute a trailing-twelve-month figure, regardless of
    -- sector. Belt and braces against the hardcoded mapping being
    -- incomplete.
    case
        when rc.quarters_with_revenue < 4 then true else false
    end as insufficient_revenue_history,

    -- Flags a company whose SEC filing history is unusually short. XOM
    -- is one: its current CIK is a recently registered entity following
    -- a corporate reorganization, so filings only reach back to 2024.
    -- Surfacing it as a column beats discovering it later as a
    -- mysterious gap.
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
