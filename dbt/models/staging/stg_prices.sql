-- Rename raw price columns and drop empty rows.

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
        volume

    from source

    -- Yahoo sometimes sends a date with no prices
    where close is not null

)

select * from renamed
