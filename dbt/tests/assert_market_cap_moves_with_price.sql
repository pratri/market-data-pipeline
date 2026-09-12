-- Day to day, market cap should move with the as-traded close times any
-- share split that day. A gap over 20% means the share count and the price
-- are on different bases: a split handled on one side only, a count
-- reported in millions, or a stale count replaced by a fresh one.

with daily as (

    select
        f.ticker,
        f.trade_date,
        f.close_price,
        f.market_cap,
        f.shares_basis,
        coalesce(s.share_split_ratio, 1) as share_split_ratio,
        lag(f.close_price) over (partition by f.ticker order by f.trade_date) as prev_close,
        lag(f.market_cap)  over (partition by f.ticker order by f.trade_date) as prev_market_cap
    from {{ ref('fct_daily_metrics') }} f
    left join {{ ref('int_stock_splits') }} s
        on  s.ticker = f.ticker
        and s.split_date = f.trade_date

)

select *
from daily
where market_cap > 0
  and prev_market_cap > 0
  and prev_close > 0
  and abs(
        (market_cap / prev_market_cap)
        / ((close_price / prev_close) * share_split_ratio)
        - 1
      ) > 0.2
