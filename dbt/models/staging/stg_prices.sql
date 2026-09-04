-- Thin layer over raw prices: rename, cast, drop load metadata.
--
-- No business logic here deliberately. Staging exists so downstream
-- models never reference source tables directly, which means a change
-- in the raw schema is absorbed in one place instead of rippling
-- through every model.

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

    -- Yahoo occasionally returns a row with a date but no price data.
    -- Excluding here rather than downstream keeps every consumer from
    -- having to remember the same filter.
    where close is not null

)

select * from renamed
