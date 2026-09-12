-- Nothing in this universe moves 50% in a day. A return that big is almost
-- certainly a split applied twice or not at all.

select
    ticker,
    trade_date,
    close_price,
    adjustment_factor,
    daily_return
from {{ ref('int_prices_daily') }}
where abs(daily_return) > 0.5
