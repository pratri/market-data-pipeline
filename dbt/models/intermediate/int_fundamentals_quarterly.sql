-- Pivots fundamentals into one row per company-quarter.
--
-- Three problems handled here, all found by auditing output rather
-- than assuming SEC's tags behave uniformly across filers.
--
-- 1. DOUBLE COUNTING. Flow metrics are reported over overlapping
--    durations sharing an end date: Q2 alone (Apr-Jun) and the
--    half-year (Jan-Jun) both end 30 June. Summing without regard to
--    duration roughly doubles revenue.
--
-- 2. CUMULATIVE REPORTING. Many filers never report a standalone Q2,
--    Q3 or Q4. They report Q1, then year-to-date H1, 9M and FY. Taking
--    only period_type='quarterly' discards most of their history: ABBV
--    kept 6 of 20 revenue rows, KO 26 of 53. Discrete quarters are
--    derived by differencing consecutive cumulative figures
--    (Q2 = H1 - Q1, Q3 = 9M - H1, Q4 = FY - 9M).
--
--    A cumulative series is identified as any (ticker, metric,
--    period_start) group with more than one row. That works for
--    non-calendar fiscal years too (AVGO ends November, INTU July,
--    DIS September), which a DATE_TRUNC-on-calendar-year approach
--    would silently mishandle.
--
-- 3. SHARES OUTSTANDING SPANS TWO SHAPES. About half the universe
--    reports CommonStockSharesOutstanding, a point-in-time balance
--    ('instant'). The rest report
--    WeightedAverageNumberOfDilutedSharesOutstanding, an average over
--    the period, which carries a period_start and lands in
--    'quarterly'. Accepting only the instant form left half the
--    universe with no share count, hence no market cap and no
--    valuation ratios at all.

with fundamentals as (

    select * from {{ ref('stg_fundamentals') }}

),

flow_metrics as (

    select
        ticker, cik, period_start, period_end, period_type,
        filed, fiscal_year, fiscal_period, metric, value
    from fundamentals
    where metric in (
        'revenue',
        'net_income',
        'operating_income',
        'operating_cash_flow'
    )
    and period_type in ('quarterly', 'half_year', 'nine_month', 'annual')

),

-- Flow values already reported as a standalone quarter. Preferred over
-- anything derived: it's what the company actually stated.
flows_as_reported as (

    select
        ticker, cik, period_end, filed, fiscal_year, fiscal_period,
        metric, value,
        'as_reported' as value_source
    from flow_metrics
    where period_type = 'quarterly'

),

-- Cumulative series: consecutive rows sharing a period_start.
cumulative_series as (

    select
        *,
        count(*) over (
            partition by ticker, metric, period_start
        ) as rows_in_series,

        lag(value) over (
            partition by ticker, metric, period_start
            order by period_end
        ) as prev_cumulative_value,

        lag(period_end) over (
            partition by ticker, metric, period_start
            order by period_end
        ) as prev_period_end

    from flow_metrics

),

flows_derived as (

    select
        ticker, cik, period_end, filed, fiscal_year, fiscal_period,
        metric,
        value - prev_cumulative_value as value,
        'derived' as value_source

    from cumulative_series
    where rows_in_series > 1
      and prev_cumulative_value is not null

      -- Guard: the gap between consecutive cumulative points should be
      -- about one quarter. A wider gap means the series skipped a
      -- filing, and differencing across it would produce a half-year
      -- figure masquerading as a quarter.
      and datediff('day', prev_period_end, period_end) between 80 and 100

),

flows_combined as (

    select * from flows_as_reported
    union all
    select * from flows_derived

),

-- Where both exist for the same quarter, keep the as-reported value.
flows_deduped as (

    select
        ticker, cik, period_end, filed, fiscal_year, fiscal_period,
        metric, value, value_source
    from (
        select
            *,
            row_number() over (
                partition by ticker, metric, period_end
                order by case when value_source = 'as_reported' then 0 else 1 end
            ) as rn
        from flows_combined
    )
    where rn = 1

),

-- Balance-sheet metrics: point-in-time, no differencing applies.
stocks as (

    select
        ticker, cik, period_end, filed, fiscal_year, fiscal_period,
        metric, value,
        'as_reported' as value_source
    from fundamentals
    where period_type = 'instant'
      and metric in (
          'total_assets',
          'total_liabilities',
          'stockholders_equity',
          'cash_and_equivalents'
      )

),

shares_instant as (

    select
        ticker, cik, period_end, filed, fiscal_year, fiscal_period,
        value as shares_value
    from fundamentals
    where metric = 'shares_outstanding'
      and period_type = 'instant'

),

-- Fallback form: an average over the quarter rather than a count at a
-- moment, so market cap derived from it is approximate. Materially
-- better than nulls, and the gap is small absent heavy buybacks or
-- issuance mid-period.
shares_weighted as (

    select
        ticker, cik, period_end, filed, fiscal_year, fiscal_period,
        value as shares_value
    from fundamentals
    where metric = 'shares_outstanding'
      and period_type = 'quarterly'

),

shares_combined as (

    select
        coalesce(i.ticker, w.ticker)               as ticker,
        coalesce(i.cik, w.cik)                     as cik,
        coalesce(i.period_end, w.period_end)       as period_end,
        coalesce(i.filed, w.filed)                 as filed,
        coalesce(i.fiscal_year, w.fiscal_year)     as fiscal_year,
        coalesce(i.fiscal_period, w.fiscal_period) as fiscal_period,
        'shares_outstanding'                       as metric,
        coalesce(i.shares_value, w.shares_value)   as value,
        case
            when i.shares_value is not null then 'instant'
            else 'weighted_average'
        end as shares_basis

    from shares_instant i
    full outer join shares_weighted w
        on  i.ticker = w.ticker
        and i.period_end = w.period_end

),

combined as (

    select ticker, cik, period_end, filed, fiscal_year, fiscal_period,
           metric, value, value_source,
           cast(null as varchar) as shares_basis
    from flows_deduped

    union all

    select ticker, cik, period_end, filed, fiscal_year, fiscal_period,
           metric, value, value_source,
           cast(null as varchar) as shares_basis
    from stocks

    union all

    select ticker, cik, period_end, filed, fiscal_year, fiscal_period,
           metric, value, 'as_reported' as value_source, shares_basis
    from shares_combined

),

pivoted as (

    select
        ticker,
        cik,
        period_end,

        -- A quarter's metrics can arrive across several filings; the
        -- latest is when the full picture became public, which is what
        -- the downstream as-of join keys on.
        max(filed) as filed_date,

        max(fiscal_year)   as fiscal_year,
        max(fiscal_period) as fiscal_period,
        max(shares_basis)  as shares_basis,

        max(case when metric = 'revenue' then value_source end)
            as revenue_source,

        max(case when metric = 'revenue'              then value end) as revenue,
        max(case when metric = 'net_income'           then value end) as net_income,
        max(case when metric = 'operating_income'     then value end) as operating_income,
        max(case when metric = 'operating_cash_flow'  then value end) as operating_cash_flow,
        max(case when metric = 'total_assets'         then value end) as total_assets,
        max(case when metric = 'total_liabilities'    then value end) as total_liabilities,
        max(case when metric = 'stockholders_equity'  then value end) as stockholders_equity,
        max(case when metric = 'shares_outstanding'   then value end) as shares_outstanding,
        max(case when metric = 'cash_and_equivalents' then value end) as cash_and_equivalents

    from combined
    group by ticker, cik, period_end

),

with_growth as (

    select
        *,
        lag(revenue) over (partition by ticker order by period_end)
            as prev_quarter_revenue,
        lag(revenue, 4) over (partition by ticker order by period_end)
            as year_ago_revenue,
        lag(net_income, 4) over (partition by ticker order by period_end)
            as year_ago_net_income
    from pivoted

)

select
    ticker,
    cik,
    period_end,
    filed_date,
    fiscal_year,
    fiscal_period,
    shares_basis,
    revenue_source,

    revenue,
    net_income,
    operating_income,
    operating_cash_flow,
    total_assets,
    total_liabilities,
    stockholders_equity,
    shares_outstanding,
    cash_and_equivalents,

    -- nullif guards divide-by-zero: a company with zero prior revenue
    -- would otherwise fail the model rather than nulling one row.
    case
        when prev_quarter_revenue is not null and prev_quarter_revenue != 0
        then (revenue - prev_quarter_revenue) / abs(nullif(prev_quarter_revenue, 0))
    end as revenue_qoq_growth,

    case
        when year_ago_revenue is not null and year_ago_revenue != 0
        then (revenue - year_ago_revenue) / abs(nullif(year_ago_revenue, 0))
    end as revenue_yoy_growth,

    case
        when year_ago_net_income is not null and year_ago_net_income != 0
        then (net_income - year_ago_net_income) / abs(nullif(year_ago_net_income, 0))
    end as net_income_yoy_growth,

    case
        when revenue is not null and revenue != 0
        then net_income / nullif(revenue, 0)
    end as net_margin

from with_growth
