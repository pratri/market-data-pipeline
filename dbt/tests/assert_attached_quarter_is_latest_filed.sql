-- The fundamentals on a trading day should come from the newest quarter
-- that was fully public by then. Any row here means the join skipped a
-- quarter people already had.

select
    f.ticker,
    f.trade_date,
    f.fundamentals_period_end,
    q.period_end as newer_period_end,
    q.filed_date as newer_filed_date
from {{ ref('fct_daily_metrics') }} f
join {{ ref('int_fundamentals_quarterly') }} q
    on  q.ticker = f.ticker
    and q.filed_date <= f.trade_date
    and q.period_end > f.fundamentals_period_end
