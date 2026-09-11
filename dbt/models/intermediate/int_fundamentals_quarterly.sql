-- One row per company per quarter.
--
-- Everything below was found by checking actual output values:
--   1. Q2 and H1 share an end date, so flows are split by period length
--      or revenue gets double counted.
--   2. Lots of filers only report YTD (Q1, H1, 9M, FY). Standalone
--      quarters are derived by differencing: Q2 = H1 - Q1, Q3 = 9M - H1,
--      Q4 = FY - 9M. Using reported quarters only, ABBV kept 6 of 20
--      revenue rows and KO 26 of 53.
--   3. About half the universe reports point-in-time shares outstanding,
--      the other half only weighted average diluted shares. Both are used,
--      otherwise half the companies get no market cap.
--   4. Some tickers have gaps in their filing history, so growth checks
--      the real dates instead of trusting lag(x, 4).

with fundamentals as (

    select * from {{ ref('stg_fundamentals') }}

),

flow_metrics as (

    select
        ticker, cik, period_start, period_end, period_type,
        filed, fiscal_year, fiscal_period, metric, tag, value
    from fundamentals
    where metric in (
        'revenue',
        'net_income',
        'operating_income',
        'operating_cash_flow'
    )
    and period_type in ('quarterly', 'half_year', 'nine_month', 'annual')

),

-- Standalone quarters as the company reported them. These win over
-- derived values.
flows_as_reported as (

    select
        ticker, cik, period_end, filed, fiscal_year, fiscal_period,
        metric, value,
        'as_reported' as value_source
    from flow_metrics
    where period_type = 'quarterly'

),

-- A YTD series is rows sharing period_start and tag. Keying on
-- period_start handles odd fiscal years (AVGO ends Nov, INTU Jul, DIS Sep).
--
-- Tag is in the key because companies switch tags mid-year. MA reported
-- Q1-Q3 2021 as Revenues and FY as SalesRevenueNet, and differencing
-- across the two gave -2.589bn of Q4 revenue. About twenty companies had
-- a mixed-tag year. Now a switch year just loses its derived quarters.
cumulative_series as (

    select
        *,
        count(*) over (
            partition by ticker, metric, period_start, tag
        ) as rows_in_series,

        lag(value) over (
            partition by ticker, metric, period_start, tag
            order by period_end
        ) as prev_cumulative_value,

        lag(period_end) over (
            partition by ticker, metric, period_start, tag
            order by period_end
        ) as prev_period_end,

        lag(filed) over (
            partition by ticker, metric, period_start, tag
            order by period_end
        ) as prev_filed

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

      -- YTD points should be one quarter apart, otherwise the difference
      -- covers more than a quarter
      and datediff('day', prev_period_end, period_end) between 80 and 100

      -- Both points should also be filed close together. IBM's 9M 2020
      -- ($53.3bn) and FY 2020 ($55.2bn) were filed 15 months apart, with
      -- the FY restated after the Kyndryl spinoff, and the difference came
      -- out as $1.9bn of Q4 revenue against a real ~$20bn.
      and datediff('day', prev_filed, filed) between 0 and 200

      -- A restated YTD number can still come in below the previous one.
      -- Revenue can't be negative so those get dropped. Operating income
      -- and cash flow can go negative, so they're left alone.
      and not (metric = 'revenue' and value - prev_cumulative_value < 0)

),

flows_combined as (

    select * from flows_as_reported
    union all
    select * from flows_derived

),

-- as-reported beats derived when both exist
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

-- balance sheet items, nothing to difference
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

-- Fallback: average shares over the quarter. Market cap from this is
-- approximate, but close unless there were big buybacks or issuance.
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

        -- MCD files weighted average diluted shares in millions (711.1)
        -- where everyone else uses units, which made its market cap
        -- $184,622. Under 1M gets nulled rather than rescaled, since
        -- guessing a scale factor from size will break eventually.
        -- Nothing in this universe has fewer than 1M shares.
        case
            when coalesce(i.shares_value, w.shares_value) >= 1000000
            then coalesce(i.shares_value, w.shares_value)
        end as value,

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

        -- a quarter's metrics can come from several filings, the downstream
        -- as-of join uses the latest one
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

-- Carry the last known share count forward.
--
-- A lot of companies only report shares outstanding in the 10-K (NVDA in
-- January, IBM December, DIS September). That left 12 of 64 with no share
-- count on their latest quarter, so no market cap and no ratios.
--
-- The fill has no age limit: carried values average 789 days old, worst
-- case 949. shares_basis marks them so they can be filtered out.
-- Forward only, since filling backward would be lookahead.
shares_filled as (

    select
        * exclude (shares_outstanding, shares_basis),

        coalesce(
            shares_outstanding,
            last_value(shares_outstanding ignore nulls) over (
                partition by ticker
                order by period_end
                rows between unbounded preceding and current row
            )
        ) as shares_outstanding,

        case
            when shares_outstanding is not null then shares_basis
            when last_value(shares_outstanding ignore nulls) over (
                     partition by ticker
                     order by period_end
                     rows between unbounded preceding and current row
                 ) is not null
            then 'carried_forward'
        end as shares_basis

    from pivoted

),

-- lag(x, 4) is four rows back, which is only four quarters when there are
-- no gaps. The lagged period_end comes along so the final select can check
-- the real distance.
with_growth as (

    select
        *,

        lag(revenue) over (partition by ticker order by period_end)
            as prev_quarter_revenue,
        lag(period_end) over (partition by ticker order by period_end)
            as prev_quarter_period_end,

        lag(revenue, 4) over (partition by ticker order by period_end)
            as year_ago_revenue,
        lag(net_income, 4) over (partition by ticker order by period_end)
            as year_ago_net_income,
        lag(period_end, 4) over (partition by ticker order by period_end)
            as year_ago_period_end

    from shares_filled

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

    -- QoQ: previous row has to be about one quarter back (same 80-100 days
    -- as the YTD differencing)
    case
        when prev_quarter_revenue is not null
         and prev_quarter_revenue != 0
         and datediff('day', prev_quarter_period_end, period_end) between 80 and 100
        then (revenue - prev_quarter_revenue) / abs(nullif(prev_quarter_revenue, 0))
    end as revenue_qoq_growth,

    -- YoY: 330-400 days leaves room for 52/53 week years but rules out the
    -- multi-year gaps that gave COP 2,825% growth
    case
        when year_ago_revenue is not null
         and year_ago_revenue != 0
         and datediff('day', year_ago_period_end, period_end) between 330 and 400
        then (revenue - year_ago_revenue) / abs(nullif(year_ago_revenue, 0))
    end as revenue_yoy_growth,

    case
        when year_ago_net_income is not null
         and year_ago_net_income != 0
         and datediff('day', year_ago_period_end, period_end) between 330 and 400
        then (net_income - year_ago_net_income) / abs(nullif(year_ago_net_income, 0))
    end as net_income_yoy_growth,

    case
        when revenue is not null and revenue != 0
        then net_income / nullif(revenue, 0)
    end as net_margin

from with_growth
