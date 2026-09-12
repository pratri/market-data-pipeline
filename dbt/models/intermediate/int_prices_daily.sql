-- Daily prices as traded, plus returns and 20-day rolling stats.
--
-- Yahoo adjusts every earlier close for splits (and some spinoffs) as of
-- the day the history is downloaded. Rows fetched on different days end up
-- on different bases: a partition written before NFLX's 10-for-1 has
-- ~$1,200 closes, one written after has ~$120 for the same day. SEC share
-- counts are as of their filing date, so neither matches them reliably.
--
-- Each row is put back to the price it actually traded at by undoing the
-- adjustments Yahoo had already applied when it was fetched. Market cap
-- uses that price. Returns use split_adjusted_close_price, which applies
-- every known adjustment, so split and spinoff days don't look like crashes.

with prices as (

    select * from {{ ref('stg_prices') }}

),

events as (

    select * from {{ ref('int_stock_splits') }}

),

-- Adjustments already in the row when it was downloaded. The DAG runs
-- before the open, so a fetch on the ex-date itself doesn't include it.
-- Yahoo treats every event as a split, so volume gets the same ratios.
applied_at_fetch as (

    select
        p.ticker,
        p.trade_date,
        exp(sum(ln(e.split_ratio))) as price_factor
    from prices p
    join events e
        on  e.ticker = p.ticker
        and e.split_date > p.trade_date
        and e.split_date < to_date(p.fetched_at)
    group by p.ticker, p.trade_date

),

-- every adjustment after the trade date that's known now
known_after as (

    select
        p.ticker,
        p.trade_date,
        exp(sum(ln(e.split_ratio))) as price_factor
    from prices p
    join events e
        on  e.ticker = p.ticker
        and e.split_date > p.trade_date
    group by p.ticker, p.trade_date

),

as_traded as (

    select
        p.trade_date,
        p.ticker,

        p.open_price  * coalesce(a.price_factor, 1) as open_price,
        p.high_price  * coalesce(a.price_factor, 1) as high_price,
        p.low_price   * coalesce(a.price_factor, 1) as low_price,
        p.close_price * coalesce(a.price_factor, 1) as close_price,
        p.volume      / coalesce(a.price_factor, 1) as volume,
        coalesce(p.dividends, 0) * coalesce(a.price_factor, 1) as dividend,

        coalesce(k.price_factor, 1) as adjustment_factor

    from prices p
    left join applied_at_fetch a
        on  a.ticker = p.ticker
        and a.trade_date = p.trade_date
    left join known_after k
        on  k.ticker = p.ticker
        and k.trade_date = p.trade_date

),

with_returns as (

    select
        *,

        close_price / adjustment_factor as split_adjusted_close_price,

        (close_price + dividend) / adjustment_factor
            / nullif(lag(close_price / adjustment_factor) over (
                partition by ticker order by trade_date
              ), 0)
            - 1 as daily_return

    from as_traded

)

select
    trade_date,
    ticker,
    open_price,
    high_price,
    low_price,
    close_price,
    split_adjusted_close_price,
    adjustment_factor,
    volume,
    dividend,
    daily_return,

    -- 20 rows is about a trading month. Rows rather than dates so
    -- weekends and holidays don't shrink the window.
    avg(daily_return) over (
        partition by ticker
        order by trade_date
        rows between 19 preceding and current row
    ) as avg_return_20d,

    stddev(daily_return) over (
        partition by ticker
        order by trade_date
        rows between 19 preceding and current row
    ) as volatility_20d,

    avg(volume) over (
        partition by ticker
        order by trade_date
        rows between 19 preceding and current row
    ) as avg_volume_20d,

    -- annualized with sqrt(252) trading days
    stddev(daily_return) over (
        partition by ticker
        order by trade_date
        rows between 19 preceding and current row
    ) * sqrt(252) as annualized_volatility_20d

from with_returns
