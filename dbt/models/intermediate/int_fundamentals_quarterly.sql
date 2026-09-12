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
--
-- `filed` is the first filing that reported a number (see
-- ingest_fundamentals.py), so it's when the number became public.

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
        'as_reported' as value_source,
        false as crosses_tags
    from flow_metrics
    where period_type = 'quarterly'

),

-- A YTD series is rows sharing period_start: Q1, H1, 9M and FY all start on
-- the first day of the fiscal year. Keying on period_start handles odd
-- fiscal years (AVGO ends Nov, INTU Jul, DIS Sep).
--
-- Companies switch tags between the 9M and the FY fairly often, usually for
-- the same number: NVDA's FY2021 10-K used RevenueFromContractWithCustomer
-- after three 10-Qs of Revenues, AVGO's FY2024 10-K used NetIncomeLoss after
-- ProfitLoss. Refusing to difference across tags lost those Q4s and a year
-- of TTM with them. But a switch can also be to a different concept: MA's
-- 2021 FY under SalesRevenueNet gave -2.589bn of Q4 revenue. So cross-tag
-- quarters are allowed but flagged, and flows_checked holds them to a
-- tighter test.
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
        ) as prev_period_end,

        lag(filed) over (
            partition by ticker, metric, period_start
            order by period_end
        ) as prev_filed,

        lag(tag) over (
            partition by ticker, metric, period_start
            order by period_end
        ) as prev_tag

    from flow_metrics

),

flows_derived as (

    select
        ticker, cik, period_end, filed, fiscal_year, fiscal_period,
        metric,
        value - prev_cumulative_value as value,
        'derived' as value_source,
        tag != prev_tag as crosses_tags

    from cumulative_series
    where rows_in_series > 1
      and prev_cumulative_value is not null

      -- YTD points should be one quarter apart, otherwise the difference
      -- covers more than a quarter. 120 allows 16-week fourth quarters.
      and datediff('day', prev_period_end, period_end) between 80 and 120

      -- Both points should also be filed close together. A FY that first
      -- shows up long after the 9M came from a later filing on a different
      -- basis. Back when filed dates were the last filing instead of the
      -- first, IBM's post-Kyndryl FY 2020 minus its original 9M came out at
      -- $1.9bn of Q4 revenue against a real ~$20bn.
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

flows_ranked as (

    select
        *,
        row_number() over (
            partition by ticker, metric, period_end, value_source
            order by filed
        ) as rn
    from flows_combined

),

as_reported_neighbors as (

    select
        ticker,
        metric,
        period_end,
        lag(value)       over (partition by ticker, metric order by period_end) as prev_value,
        lag(period_end)  over (partition by ticker, metric order by period_end) as prev_end,
        lead(value)      over (partition by ticker, metric order by period_end) as next_value,
        lead(period_end) over (partition by ticker, metric order by period_end) as next_end
    from flows_ranked
    where value_source = 'as_reported'
      and rn = 1

),

-- As-reported beats derived when both exist, unless they disagree by more
-- than 20%. Then whichever is closer to the reported quarters either side
-- wins, because either one can be the bad number:
--   GE's 10-Ks tag a 91-day Revenues fact of $2.585bn for Q4 2015 (and
--   $2.649bn for Q4 2016) against ~$30bn quarters around it. That line isn't
--   total revenue, and it made Q1 look like 977% QoQ growth. Derived wins.
--   Around a spinoff it goes the other way: the FY or H1 figure is restated
--   without the spun-off business and the earlier YTD figure isn't, so the
--   difference is too small (GE Q2 2024 at Vernova, JNJ Q3 2023 at Kenvue,
--   HON Q4 2025 at Solstice). As-reported wins.
-- With no neighbours to compare against, as-reported wins.
flows_paired as (

    select
        coalesce(a.ticker, d.ticker)         as ticker,
        coalesce(a.metric, d.metric)         as metric,
        coalesce(a.period_end, d.period_end) as period_end,

        coalesce(
            a.value is null
            or (
                d.value is not null
                and abs(a.value - d.value) > 0.2 * greatest(abs(a.value), abs(d.value))
                and abs(d.value - n.neighbor_value) < abs(a.value - n.neighbor_value)
            ),
            false
        ) as use_derived,

        a.cik           as a_cik,           d.cik           as d_cik,
        a.filed         as a_filed,         d.filed         as d_filed,
        a.fiscal_year   as a_fiscal_year,   d.fiscal_year   as d_fiscal_year,
        a.fiscal_period as a_fiscal_period, d.fiscal_period as d_fiscal_period,
        a.value         as a_value,         d.value         as d_value,
        d.crosses_tags  as d_crosses_tags

    from (select * from flows_ranked where value_source = 'as_reported' and rn = 1) a
    full outer join (select * from flows_ranked where value_source = 'derived' and rn = 1) d
        on  a.ticker = d.ticker
        and a.metric = d.metric
        and a.period_end = d.period_end
    left join (
        select
            ticker,
            metric,
            period_end,
            case
                when datediff('day', prev_end, period_end) between 80 and 120
                 and datediff('day', period_end, next_end) between 80 and 120
                then (prev_value + next_value) / 2
                when datediff('day', prev_end, period_end) between 80 and 120
                then prev_value
                when datediff('day', period_end, next_end) between 80 and 120
                then next_value
            end as neighbor_value
        from as_reported_neighbors
    ) n
        on  n.ticker = a.ticker
        and n.metric = a.metric
        and n.period_end = a.period_end

),

flows_deduped as (

    select
        ticker,
        case when use_derived then d_cik else a_cik end                     as cik,
        period_end,
        case when use_derived then d_filed else a_filed end                 as filed,
        case when use_derived then d_fiscal_year else a_fiscal_year end     as fiscal_year,
        case when use_derived then d_fiscal_period else a_fiscal_period end as fiscal_period,
        metric,
        case when use_derived then d_value else a_value end                 as value,
        case when use_derived then 'derived' else 'as_reported' end         as value_source,
        case when use_derived then d_crosses_tags else false end            as crosses_tags
    from flows_paired

),

flows_neighbors as (

    select
        *,
        lag(value)       over (partition by ticker, metric order by period_end) as prev_value,
        lag(period_end)  over (partition by ticker, metric order by period_end) as prev_end,
        lead(value)      over (partition by ticker, metric order by period_end) as next_value,
        lead(period_end) over (partition by ticker, metric order by period_end) as next_end
    from flows_deduped

),

-- Derived revenue far out of line with the quarters either side means the
-- two YTD figures straddle a spinoff and there's no reported quarter to fall
-- back on. IBM's FY 2021 10-K left out Kyndryl while its 9M figure didn't,
-- so Q4 came out at $3.3bn against a real ~$16.7bn. Those are dropped.
-- A quarter differenced across two tags has to land within 0.5-2x of its
-- neighbours, and needs neighbours to check against at all.
-- Revenue only, since income and cash flow can swing sign legitimately.
flows_checked as (

    select
        ticker, cik, period_end, filed, fiscal_year, fiscal_period,
        metric, value, value_source
    from (
        select
            *,
            case
                when datediff('day', prev_end, period_end) between 80 and 120
                 and datediff('day', period_end, next_end) between 80 and 120
                then (prev_value + next_value) / 2
                when datediff('day', prev_end, period_end) between 80 and 120
                then prev_value
                when datediff('day', period_end, next_end) between 80 and 120
                then next_value
            end as neighbor_value
        from flows_neighbors
    )
    where not coalesce(
        metric = 'revenue'
        and value_source = 'derived'
        and neighbor_value > 0
        and (value < 0.3 * neighbor_value or value > 3 * neighbor_value),
        false
    )
    and not (
        metric = 'revenue'
        and value_source = 'derived'
        and coalesce(crosses_tags, false)
        and not coalesce(value between 0.5 * neighbor_value and 2 * neighbor_value, false)
    )

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
        case when i.shares_value is not null then i.filed else w.filed end
                                                   as filed,
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
    from flows_checked

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
        period_end,

        -- XOM and BLK span two CIKs; take the one from the latest filing
        max_by(cik, filed) as cik,

        -- a quarter's metrics can come from several filings, the downstream
        -- as-of join uses the latest one
        max(filed) as filed_date,

        max(fiscal_year)   as fiscal_year,
        max(fiscal_period) as fiscal_period,
        max(shares_basis)  as shares_basis,

        max(case when metric = 'revenue' then value_source end)
            as revenue_source,

        -- the share count's own filing date, used to line it up with splits
        max(case when metric = 'shares_outstanding' and value is not null then filed end)
            as shares_filed_date,

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
    group by ticker, period_end

),

-- Drop stray dates right next to a real quarter end that carry no income
-- statement data, like the equity balance CAT and BA tag at Jan 1 after a
-- Dec 31 year end. They'd take a slot in the 4-row TTM window and lag(x, 4).
quarter_rows as (

    select * exclude (prev_end, next_end)
    from (
        select
            *,
            lag(period_end)  over (partition by ticker order by period_end) as prev_end,
            lead(period_end) over (partition by ticker order by period_end) as next_end
        from pivoted
    )
    where not (
        revenue is null
        and net_income is null
        and operating_income is null
        and operating_cash_flow is null
        and (
            coalesce(datediff('day', prev_end, period_end) < 15, false)
            or coalesce(datediff('day', period_end, next_end) < 15, false)
        )
    )

),

-- Carry the last known share count forward.
--
-- A lot of companies only report shares outstanding in the 10-K (NVDA in
-- January, IBM December, DIS September). That left 12 of 64 with no share
-- count on their latest quarter. The fill has no age limit, and
-- shares_basis marks carried rows. int_market_cap_shares prefers the cover
-- page count when it's more recent.
-- Forward only, since filling backward would be lookahead.
shares_filled as (

    select
        * exclude (shares_outstanding, shares_basis, shares_filed_date),

        coalesce(
            shares_outstanding,
            last_value(shares_outstanding ignore nulls) over (
                partition by ticker
                order by period_end
                rows between unbounded preceding and current row
            )
        ) as shares_outstanding,

        coalesce(
            shares_filed_date,
            last_value(shares_filed_date ignore nulls) over (
                partition by ticker
                order by period_end
                rows between unbounded preceding and current row
            )
        ) as shares_filed_date,

        case
            when shares_outstanding is not null then shares_basis
            when last_value(shares_outstanding ignore nulls) over (
                     partition by ticker
                     order by period_end
                     rows between unbounded preceding and current row
                 ) is not null
            then 'carried_forward'
        end as shares_basis

    from quarter_rows

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
    shares_filed_date,
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

    -- QoQ: previous row has to be about one quarter back (same 80-120 days
    -- as the YTD differencing)
    case
        when prev_quarter_revenue is not null
         and prev_quarter_revenue != 0
         and datediff('day', prev_quarter_period_end, period_end) between 80 and 120
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
