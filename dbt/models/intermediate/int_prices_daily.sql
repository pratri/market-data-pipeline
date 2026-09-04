-- Daily returns and rolling volatility per ticker.
--
-- Returns use adj_close where available, falling back to close.
-- Adjusted close accounts for splits and dividends; using raw close
-- would show a 50% "loss" on a 2-for-1 split day that never happened.

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

    -- 20 trading days is roughly one calendar month. The window is
    -- bounded by rows rather than dates because markets close on
    -- weekends and holidays, so a date-based window would silently
    -- include fewer observations in some periods.
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

    -- Annualized from daily using sqrt(252), the conventional count of
    -- US trading days in a year.
    stddev(daily_return) over (
        partition by ticker
        order by trade_date
        rows between 19 preceding and current row
    ) * sqrt(252) as annualized_volatility_20d

from with_returns
