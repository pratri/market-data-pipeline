-- TTM figures on a trading day should only use quarters that were all public
-- by then. The join only checks the attached quarter, so an earlier quarter
-- tagged in a later filing could slip into the window.

with quarters as (

    select
        ticker,
        period_end,
        filed_date,
        row_number() over (partition by ticker order by period_end desc) as rn
    from {{ ref('int_fundamentals_quarterly') }}
    where filed_date is not null

),

attached as (

    select
        f.ticker,
        f.trade_date,
        q.rn
    from {{ ref('fct_daily_metrics') }} f
    join quarters q
        on  q.ticker = f.ticker
        and q.period_end = f.fundamentals_period_end
    where f.ttm_revenue is not null
       or f.ttm_net_income is not null

)

select
    a.ticker,
    a.trade_date,
    max(q.filed_date) as latest_input_filed_date
from attached a
join quarters q
    on  q.ticker = a.ticker
    and q.rn between a.rn and a.rn + 3
group by a.ticker, a.trade_date
having max(q.filed_date) > a.trade_date
