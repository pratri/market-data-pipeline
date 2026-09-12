-- Latest fundamentals snapshot per ticker, with each fact labelled by
-- period length.
--
-- Raw keeps every weekly snapshot. Each ticker reads the newest snapshot it
-- appears in, so a company SEC didn't return this week keeps last week's
-- data instead of vanishing, and a --limit test run doesn't wipe everyone
-- else.
--
-- Period length matters because Q2 (Apr-Jun) and H1 (Jan-Jun) share an end
-- date, and grouping on end date alone double counts. The buckets are wide
-- enough for 12/12/12/16-week calendars (COST, PEP), where Q4 runs 111-118
-- days, H1 167 and 9M 251.

with source as (

    select
        *,
        regexp_substr(
            source_file,
            'snapshot_date=([0-9]{4}-[0-9]{2}-[0-9]{2})',
            1, 1, 'e', 1
        ) as snapshot_date
    from {{ source('raw', 'fundamentals') }}

),

latest_snapshot as (

    select *
    from source
    qualify snapshot_date = max(snapshot_date) over (partition by ticker)

),

-- a snapshot file overwritten by a same-day rerun gets loaded twice
deduped as (

    select *
    from latest_snapshot
    qualify row_number() over (
        partition by ticker, metric, period_start, period_end
        order by loaded_at desc
    ) = 1

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
            when datediff('day', period_start, period_end) between 80 and 120
                then 'quarterly'
            when datediff('day', period_start, period_end) between 165 and 195
                then 'half_year'
            when datediff('day', period_start, period_end) between 245 and 285
                then 'nine_month'
            when datediff('day', period_start, period_end) between 350 and 380
                then 'annual'
            else 'other'
        end as period_type

    from deduped

)

select * from classified
