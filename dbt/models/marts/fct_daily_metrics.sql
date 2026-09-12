-- Daily prices joined to the latest fundamentals filed on or before each
-- trade date.
--
-- The join is on filed date, not period end. Q2 ends June 30 but isn't
-- reported until August, so joining on period end would put earnings into
-- July rows before anyone had them (lookahead bias). Ingestion keeps the
-- first filing of each number, so filed date really is when it became
-- public, and values are as originally reported.
--
-- close_price is the price the stock actually traded at, and share counts
-- are adjusted for any split since they were filed, so market cap is right
-- on both sides of a split (int_prices_daily, int_market_cap_shares).
--
-- Revenue ratios are nulled where revenue isn't comparable, see
-- dim_companies.

with prices as (

    select * from {{ ref('int_prices_daily') }}

),

fundamentals as (

    select * from {{ ref('int_fundamentals_quarterly') }}
    where filed_date is not null

),

shares as (

    select * from {{ ref('int_market_cap_shares') }}

),

companies as (

    select
        ticker,
        sector,
        revenue_metric_applicable
    from {{ ref('dim_companies') }}

),

joined as (

    select
        p.trade_date,
        p.ticker,
        p.open_price,
        p.high_price,
        p.low_price,
        p.close_price,
        p.split_adjusted_close_price,
        p.volume,
        p.daily_return,
        p.volatility_20d,
        p.annualized_volatility_20d,
        p.avg_volume_20d,

        f.period_end   as fundamentals_period_end,
        f.filed_date   as fundamentals_filed_date,
        f.fiscal_year,
        f.fiscal_period,
        f.revenue_source,
        f.revenue,
        f.net_income,
        f.operating_income,
        f.operating_cash_flow,
        f.total_assets,
        f.total_liabilities,
        f.stockholders_equity,
        f.cash_and_equivalents,
        f.revenue_qoq_growth,
        f.revenue_yoy_growth,
        f.net_income_yoy_growth,
        f.net_margin,

        -- Latest quarter whose numbers were all public by the trade date.
        -- Ordering by filed date instead would let an old quarter win on the
        -- day one of its figures was first tagged in a later filing.
        row_number() over (
            partition by p.ticker, p.trade_date
            order by f.period_end desc
        ) as filing_recency

    from prices p
    left join fundamentals f
        on  p.ticker = f.ticker
        and f.filed_date <= p.trade_date

),

most_recent as (

    select * from joined
    where filing_recency = 1

),

ttm as (

    select
        ticker,
        period_end,

        sum(revenue) over (
            partition by ticker order by period_end
            rows between 3 preceding and current row
        ) as ttm_revenue,

        sum(net_income) over (
            partition by ticker order by period_end
            rows between 3 preceding and current row
        ) as ttm_net_income,

        count(revenue) over (
            partition by ticker order by period_end
            rows between 3 preceding and current row
        ) as revenue_quarters_in_ttm,

        count(net_income) over (
            partition by ticker order by period_end
            rows between 3 preceding and current row
        ) as income_quarters_in_ttm,

        -- four consecutive quarter ends span ~273 days
        datediff(
            'day',
            min(period_end) over (
                partition by ticker order by period_end
                rows between 3 preceding and current row
            ),
            period_end
        ) as ttm_span_days

    from fundamentals

),

final as (

    select
        m.trade_date,
        m.ticker,
        c.sector,

        m.open_price,
        m.high_price,
        m.low_price,
        m.close_price,
        m.split_adjusted_close_price,
        m.volume,
        m.daily_return,
        m.volatility_20d,
        m.annualized_volatility_20d,
        m.avg_volume_20d,

        m.fundamentals_period_end,
        m.fundamentals_filed_date,
        m.fiscal_year,
        m.fiscal_period,
        s.shares_basis,
        m.revenue_source,

        m.revenue,
        m.net_income,
        m.operating_income,
        m.operating_cash_flow,
        m.total_assets,
        m.total_liabilities,
        m.stockholders_equity,
        s.shares_outstanding,
        m.cash_and_equivalents,

        m.revenue_qoq_growth,
        m.revenue_yoy_growth,
        m.net_income_yoy_growth,
        m.net_margin,

        -- needs four consecutive quarters, a partial TTM understates
        case
            when t.revenue_quarters_in_ttm = 4
             and t.ttm_span_days between 250 and 290
             and c.revenue_metric_applicable
            then t.ttm_revenue
        end as ttm_revenue,

        case
            when t.income_quarters_in_ttm = 4
             and t.ttm_span_days between 250 and 290
            then t.ttm_net_income
        end as ttm_net_income,

        m.close_price * s.shares_outstanding as market_cap,

        -- TTM earnings. Null when earnings are negative: a negative P/E
        -- doesn't mean anything and it wrecks averages.
        case
            when t.income_quarters_in_ttm = 4
             and t.ttm_span_days between 250 and 290
             and t.ttm_net_income > 0
             and s.shares_outstanding > 0
            then (m.close_price * s.shares_outstanding) / nullif(t.ttm_net_income, 0)
        end as pe_ratio,

        -- P/B works for banks, P/S doesn't
        case
            when m.stockholders_equity > 0 and s.shares_outstanding > 0
            then (m.close_price * s.shares_outstanding)
                 / nullif(m.stockholders_equity, 0)
        end as price_to_book,

        case
            when t.revenue_quarters_in_ttm = 4
             and t.ttm_span_days between 250 and 290
             and t.ttm_revenue > 0
             and s.shares_outstanding > 0
             and c.revenue_metric_applicable
            then (m.close_price * s.shares_outstanding) / nullif(t.ttm_revenue, 0)
        end as price_to_sales,

        -- total liabilities over total assets, not just debt
        case
            when m.total_assets > 0
            then m.total_liabilities / nullif(m.total_assets, 0)
        end as debt_to_assets,

        -- ~90 means the next report is due, much higher means a gap
        datediff('day', m.fundamentals_filed_date, m.trade_date)
            as days_since_filing

    from most_recent m
    left join companies c
        on m.ticker = c.ticker
    left join shares s
        on  s.ticker = m.ticker
        and s.trade_date = m.trade_date
    left join ttm t
        on  m.ticker = t.ticker
        and m.fundamentals_period_end = t.period_end

)

select * from final
