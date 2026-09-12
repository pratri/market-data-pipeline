-- One row per price adjustment Yahoo reports on its ex-date (10 = 10-for-1,
-- 0.1 = 1-for-10 reverse split).
--
-- Yahoo books some spinoffs the same way, as fractional "splits" (HON 1.061
-- in Oct 2025, SPGI 1.057 in Jul 2026), so its adjusted chart lines up.
-- Those move the price but not the share count. share_split_ratio is the
-- part that does change the share count:
--   - a clean ratio (10, 3/2, 5/4, 1/10) is a real split
--   - otherwise, if the cover page share count on the first filing after
--     the event differs from the last one before by a clean ratio, it was a
--     split and a spinoff on the same day. HON's 0.9535 in June 2026 was a
--     1-for-2 reverse split plus a spinoff: 633.7M shares before, 316.9M after.
--   - otherwise null, a plain spinoff
--
-- Only covers the loaded price history, so earlier splits aren't known.

with events as (

    select
        ticker,
        trade_date   as split_date,
        stock_splits as split_ratio
    from {{ ref('stg_prices') }}
    where stock_splits > 0

),

cover_counts as (

    select ticker, filed, value as shares
    from {{ ref('stg_fundamentals') }}
    where metric = 'cover_shares_outstanding'
      and value >= 1000000

),

count_before as (

    select
        e.ticker,
        e.split_date,
        c.shares,
        row_number() over (partition by e.ticker, e.split_date order by c.filed desc) as rn
    from events e
    join cover_counts c
        on  c.ticker = e.ticker
        and c.filed < e.split_date
        and datediff('day', c.filed, e.split_date) <= 200

),

count_after as (

    select
        e.ticker,
        e.split_date,
        c.shares,
        row_number() over (partition by e.ticker, e.split_date order by c.filed) as rn
    from events e
    join cover_counts c
        on  c.ticker = e.ticker
        and c.filed > e.split_date
        and datediff('day', e.split_date, c.filed) <= 200

),

with_observed as (

    select
        e.ticker,
        e.split_date,
        e.split_ratio,
        a.shares / b.shares as observed_share_ratio
    from events e
    left join count_before b
        on  b.ticker = e.ticker
        and b.split_date = e.split_date
        and b.rn = 1
    left join count_after a
        on  a.ticker = e.ticker
        and a.split_date = e.split_date
        and a.rn = 1

)

select
    ticker,
    split_date,
    split_ratio,
    observed_share_ratio,

    case
        when abs(split_ratio * 4 - round(split_ratio * 4)) < 0.001
          or abs(4 / split_ratio - round(4 / split_ratio)) < 0.001
        then split_ratio

        when abs(observed_share_ratio - 1) > 0.2
         and abs(observed_share_ratio * 4 - round(observed_share_ratio * 4)) < 0.04
        then round(observed_share_ratio * 4) / 4

        when abs(observed_share_ratio - 1) > 0.2
         and abs(4 / observed_share_ratio - round(4 / observed_share_ratio)) < 0.04
        then 4 / round(4 / observed_share_ratio)
    end as share_split_ratio

from with_observed
