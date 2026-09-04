-- Staging over raw fundamentals. Two things happen here that aren't
-- cosmetic, so they're worth explaining.
--
-- 1. LATEST SNAPSHOT ONLY. The ingestion writes a dated snapshot on
--    every run, so raw holds several full copies of SEC's data as it
--    stood on different days. That history is deliberate and useful,
--    but current-value models must read one snapshot or every figure
--    multiplies.
--
-- 2. PERIOD LENGTH. SEC reports the same metric over different
--    durations that share an end date: Q2 alone (Apr-Jun) and the
--    half-year (Jan-Jun) both end 30 June. Joining on end date without
--    accounting for duration double-counts revenue. Classifying the
--    period here means downstream models filter on an explicit label
--    instead of rediscovering the trap.

with source as (

    select * from {{ source('raw', 'fundamentals') }}

),

latest_snapshot as (

    -- The snapshot date is encoded in the S3 path
    -- (raw/fundamentals/snapshot_date=YYYY-MM-DD/), so it's recovered
    -- from source_file rather than stored as a column.
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

        -- Null start means a point-in-time balance (total assets,
        -- shares outstanding) rather than a flow measured over a span.
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
