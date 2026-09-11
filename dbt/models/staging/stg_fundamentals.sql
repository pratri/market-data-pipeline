-- Latest fundamentals snapshot, with each fact labelled by period length.
--
-- Raw keeps every weekly snapshot, so only the newest is used here.
-- Period length matters because Q2 (Apr-Jun) and H1 (Jan-Jun) share an
-- end date, and grouping on end date alone double counts.

with source as (

    select * from {{ source('raw', 'fundamentals') }}

),

latest_snapshot as (

    -- snapshot date only lives in the S3 path (snapshot_date=YYYY-MM-DD)
    select
        max(
            regexp_substr(
                source_file,
                'snapshot_date=([0-9]{4}-[0-9]{2}-[0-9]{2})',
                1, 1, 'e', 1
            )
        ) as latest_snapshot_date
    from source

),

filtered as (

    select source.*
    from source
    cross join latest_snapshot
    where regexp_substr(
              source.source_file,
              'snapshot_date=([0-9]{4}-[0-9]{2}-[0-9]{2})',
              1, 1, 'e', 1
          ) = latest_snapshot.latest_snapshot_date

),

classified as (

    select
        ticker,
        cik,
        metric,
        tag,
        unit,
        period_start,
        period_end,
        value,
        fiscal_year,
        fiscal_period,
        form,
        filed,

        -- null start = point-in-time value (assets, shares), not a flow
        case
            when period_start is null then null
            else datediff('day', period_start, period_end)
        end as period_days,

        case
            when period_start is null then 'instant'
            when datediff('day', period_start, period_end) between 80 and 100
                then 'quarterly'
            when datediff('day', period_start, period_end) between 170 and 195
                then 'half_year'
            when datediff('day', period_start, period_end) between 260 and 285
                then 'nine_month'
            when datediff('day', period_start, period_end) between 350 and 380
                then 'annual'
            else 'other'
        end as period_type

    from filtered

)

select * from classified
