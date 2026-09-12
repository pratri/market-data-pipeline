-- Rename raw price columns, drop empty rows, keep one row per ticker per day.
--
-- A partition rewritten in S3 (missing tickers added, or a manual backfill)
-- gets loaded again by COPY INTO, so raw can hold the same day twice. The
-- most recently fetched copy wins.

with source as (

    select * from {{ source('raw', 'prices') }}

),

renamed as (

    select
        trade_date,
        ticker,
        open        as open_price,
        high        as high_price,
        low         as low_price,
        close       as close_price,
        adj_close   as adj_close_price,
        volume,
        dividends,
        stock_splits,
        fetched_at,
        loaded_at

    from source

    -- Yahoo sometimes sends a date with no prices
    where close is not null

),

deduped as (

    select
        *,
        row_number() over (
            partition by ticker, trade_date
            order by fetched_at desc nulls last, loaded_at desc
        ) as copy_rank
    from renamed

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
    dividends,
    stock_splits,
    fetched_at
from deduped
where copy_rank = 1
