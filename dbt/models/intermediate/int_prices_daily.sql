-- Daily returns and 20-day rolling stats per ticker.
--
-- Returns use adj_close when it's there, so dividends and splits don't
-- look like price moves. That only holds for splits if the history was
-- downloaded after the split happened.

with prices as (

    select * from {{ ref('stg_prices') }}

),

with_lags as (

    select
        trade_date,
        ticker,
        open_price,
        high_price,
        low_price,
        close_price,
        adj_close_price,
        volume,

        coalesce(adj_close_price, close_price) as return_basis,

        lag(coalesce(adj_close_price, close_price)) over (
            partition by ticker order by trade_date
        ) as prev_close

    from prices

),

with_returns as (

    select
        *,

        case
            when prev_close is not null and prev_close != 0
            then (return_basis - prev_close) / nullif(prev_close, 0)
        end as daily_return

    from with_lags

)

select
    trade_date,
    ticker,
    open_price,
    high_price,
    low_price,
    close_price,
    adj_close_price,
    volume,
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
