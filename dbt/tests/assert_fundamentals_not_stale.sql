-- Large filers report within 40 days of quarter end (60 for a 10-K), so
-- the attached quarter shouldn't be more than ~200 days old. When it is,
-- either SEC's data is missing a filing (C's 2026 10-Qs weren't in
-- companyfacts in Sept 2026) or filed dates are wrong, which is how AAPL
-- once carried a 2023 quarter into 2026 while days_since_filing looked
-- normal. Warn only, since the first case isn't ours to fix.
{{ config(severity='warn') }}

select
    ticker,
    trade_date,
    fundamentals_period_end,
    fundamentals_filed_date
from {{ ref('fct_daily_metrics') }}
where datediff('day', fundamentals_period_end, trade_date) > 200
