-- Share count used for market cap on each trading day.
--
-- Two candidates, each as of what had been filed by that day:
--   cover_page  dei:EntityCommonStockSharesOutstanding from the front of the
--               latest 10-Q/10-K. An actual count, dated a few weeks after
--               quarter end. Multi-class companies (GOOGL, META, V) only
--               report it per class, so they don't have one.
--   quarterly   the count on the latest quarter in int_fundamentals_quarterly
--               (instant, weighted average or carried forward).
-- Whichever was filed more recently wins. Counts filed more than 400 days
-- before the trade date aren't used, since a split before the loaded price
-- history couldn't be corrected for.
--
-- Prices in int_prices_daily are as traded, so a count only needs
-- adjusting for real splits between its filing date and the trade date. A
-- filing already reflects splits that happened before it was filed.

with prices as (

    select ticker, trade_date from {{ ref('int_prices_daily') }}

),

share_splits as (

    select * from {{ ref('int_stock_splits') }}
    where share_split_ratio is not null

),

cover_raw as (

    select ticker, period_end, filed, value as shares
    from {{ ref('stg_fundamentals') }}
    where metric = 'cover_shares_outstanding'
      and value >= 1000000

),

-- A cover count is only trusted when a us-gaap share count in the same
-- filing agrees with it. Multi-class companies put one class on the cover:
-- MA's says 122.5M against ~876M actual, V's says 469M against ~1.87B
-- as-converted, UPS's leaves out Class A. Using those would divide market
-- cap by 7, 4 and 1.2. Same filing means same split basis, so the two are
-- directly comparable.
--
-- The count in the filing is allowed to be a million times smaller, because
-- MCD reports its weighted average shares in millions (711.1 against a cover
-- count of 710,398,642). Without that, MCD has nothing to check against and
-- loses its market cap.
cover as (

    select c.ticker, c.period_end, c.filed, c.shares
    from cover_raw c
    join {{ ref('stg_fundamentals') }} f
        on  f.ticker = c.ticker
        and f.filed = c.filed
        and f.metric = 'shares_outstanding'
        and f.value > 0
    group by c.ticker, c.period_end, c.filed, c.shares
    having min(least(
        abs(c.shares / f.value - 1),
        abs(c.shares / (f.value * 1000000) - 1)
    )) <= 0.1
    qualify row_number() over (
        partition by c.ticker, c.filed
        order by c.period_end desc
    ) = 1

),

quarterly as (

    select
        ticker,
        period_end,
        filed_date,
        shares_outstanding,
        shares_basis,
        shares_filed_date
    from {{ ref('int_fundamentals_quarterly') }}
    where filed_date is not null

),

cover_ranked as (

    select
        p.ticker,
        p.trade_date,
        c.filed  as cover_filed,
        c.shares as cover_shares,
        row_number() over (
            partition by p.ticker, p.trade_date
            order by c.filed desc
        ) as rn
    from prices p
    join cover c
        on  c.ticker = p.ticker
        and c.filed <= p.trade_date
        and datediff('day', c.filed, p.trade_date) <= 400

),

-- same row choice as the fundamentals join in fct_daily_metrics
quarterly_ranked as (

    select
        p.ticker,
        p.trade_date,
        q.shares_outstanding,
        q.shares_basis,
        q.shares_filed_date,
        row_number() over (
            partition by p.ticker, p.trade_date
            order by q.period_end desc
        ) as rn
    from prices p
    join quarterly q
        on  q.ticker = p.ticker
        and q.filed_date <= p.trade_date

),

quarterly_asof as (

    select *
    from quarterly_ranked
    where rn = 1
      and shares_outstanding is not null
      and datediff('day', shares_filed_date, trade_date) <= 400

),

chosen as (

    select
        p.ticker,
        p.trade_date,

        case
            when c.cover_shares is not null
             and (q.shares_outstanding is null or c.cover_filed >= q.shares_filed_date)
            then 'cover_page'
            else q.shares_basis
        end as shares_basis,

        case
            when c.cover_shares is not null
             and (q.shares_outstanding is null or c.cover_filed >= q.shares_filed_date)
            then c.cover_shares
            else q.shares_outstanding
        end as shares_as_filed,

        case
            when c.cover_shares is not null
             and (q.shares_outstanding is null or c.cover_filed >= q.shares_filed_date)
            then c.cover_filed
            else q.shares_filed_date
        end as shares_filed_date

    from prices p
    left join cover_ranked c
        on  c.ticker = p.ticker
        and c.trade_date = p.trade_date
        and c.rn = 1
    left join quarterly_asof q
        on  q.ticker = p.ticker
        and q.trade_date = p.trade_date

),

split_factors as (

    select
        ch.ticker,
        ch.trade_date,
        exp(sum(ln(s.share_split_ratio))) as split_factor
    from chosen ch
    join share_splits s
        on  s.ticker = ch.ticker
        and s.split_date > ch.shares_filed_date
        and s.split_date <= ch.trade_date
    group by ch.ticker, ch.trade_date

)

select
    ch.ticker,
    ch.trade_date,
    ch.shares_basis,
    ch.shares_filed_date,
    ch.shares_as_filed,
    coalesce(sf.split_factor, 1)                      as split_factor,
    ch.shares_as_filed * coalesce(sf.split_factor, 1) as shares_outstanding
from chosen ch
left join split_factors sf
    on  sf.ticker = ch.ticker
    and sf.trade_date = ch.trade_date
where ch.shares_as_filed is not null
