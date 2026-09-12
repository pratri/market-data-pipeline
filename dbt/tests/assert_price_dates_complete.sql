-- Every trading day should have close to the whole universe. A short day is
-- a partition that lost a batch; the next --skip-existing run should add
-- the missing tickers. Warn only, since the newest day can be mid-load.
{{ config(severity='warn') }}

with counts as (

    select trade_date, count(*) as tickers
    from {{ ref('stg_prices') }}
    group by trade_date

)

select *
from counts
where tickers < (select max(tickers) from counts) * 0.95
