-- Nothing in this universe moves 50% in a day, so a return that big is a
-- split applied twice or not at all. Right after a known split the bar is
-- 15%, low enough to catch a mishandled 3-for-2 or 5-for-4 (the biggest real
-- move near a split in the fixture is SPGI's 7.7%).

select
    p.ticker,
    p.trade_date,
    p.close_price,
    p.adjustment_factor,
    p.daily_return,
    s.split_date
from {{ ref('int_prices_daily') }} p
left join {{ ref('int_stock_splits') }} s
    on  s.ticker = p.ticker
    and datediff('day', s.split_date, p.trade_date) between 0 and 3
where abs(p.daily_return) > 0.5
   or (s.split_date is not null and abs(p.daily_return) > 0.15)
