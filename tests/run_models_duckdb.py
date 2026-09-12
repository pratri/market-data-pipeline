"""Build the dbt models on DuckDB against fixture data and check the output.

`dbt parse` in CI resolves refs and Jinja but never runs the SQL, so a model
can be wrong in every way that matters and still pass. This builds all of
them over a small fixture (21 tickers pulled from SEC and Yahoo, in
tests/fixtures/), runs the tests declared in the .yml files and in
dbt/tests/, then checks values that have a known right answer.

It is not Snowflake. The SQL is rewritten where the two differ (see
translate), so this catches logic and value regressions, not dialect
problems. `sqlfluff parse` covers some of the rest.

    python tests/run_models_duckdb.py
"""

import pathlib
import re
import sys

import duckdb
import pandas as pd
import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent
MODELS = ROOT / "dbt" / "models"
SINGULAR_TESTS = ROOT / "dbt" / "tests"
FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures"

failures: list[str] = []
warnings: list[str] = []


def translate(sql: str) -> str:
    """Rewrite dbt Jinja and the Snowflake-only bits DuckDB spells differently."""
    sql = re.sub(r"\{\{\s*config\(.*?\)\s*\}\}", "", sql, flags=re.S)
    sql = re.sub(r"\{\{\s*ref\('(\w+)'\)\s*\}\}", r"\1", sql)
    sql = re.sub(r"\{\{\s*source\('raw',\s*'(\w+)'\)\s*\}\}", r"raw_\1", sql)
    sql = re.sub(
        r"regexp_substr\(\s*(.*?),\s*('[^']*'),\s*1,\s*1,\s*'e',\s*1\s*\)",
        r"regexp_extract(\1, \2, 1)", sql, flags=re.S,
    )
    sql = sql.replace("current_date()", "current_date")
    sql = re.sub(r"dateadd\('month',\s*(-?\d+),\s*current_date\)",
                 r"(current_date + interval (\1) month)", sql)
    sql = re.sub(r"\bto_date\(", "cast_date(", sql)

    def values_clause(m):
        body = m.group(1).strip().rstrip(",")
        ncols = body.split(")")[0].count(",") + 1
        cols = ", ".join(f"column{i + 1}" for i in range(ncols))
        return f"from (values {body}) as v({cols})\n"

    sql = re.sub(r"from values\s+((?:\([^()]*\)\s*,?\s*)+)", values_clause, sql)
    if "{{" in sql:
        raise AssertionError(f"unhandled Jinja: {sql[:200]}")
    return sql


def model_order(paths: dict) -> list:
    """Models in dependency order, from the ref() calls in each file."""
    deps = {
        name: {r for r in re.findall(r"ref\('(\w+)'\)", p.read_text()) if r in paths}
        for name, p in paths.items()
    }
    ordered: list = []
    while len(ordered) < len(paths):
        ready = sorted(n for n in paths if n not in ordered and deps[n] <= set(ordered))
        if not ready:
            raise AssertionError(f"circular refs among {set(paths) - set(ordered)}")
        ordered += ready
    return ordered


def load_fixtures(con, prices, fundamentals) -> None:
    con.register("p_df", prices)
    con.register("f_df", fundamentals)
    con.execute("""
        create or replace table raw_prices as
        select cast(date as date) as trade_date, ticker, open, high, low, close, adj_close,
               cast(volume as double) as volume, dividends, stock_splits,
               cast(fetched_at as timestamp) as fetched_at,
               'raw/prices/date=' || cast(date as varchar) || '/prices.parquet' as source_file,
               cast(loaded_at as timestamp) as loaded_at
        from p_df""")
    con.execute("""
        create or replace table raw_fundamentals as
        select ticker, cik, metric, tag, unit,
               try_cast(period_start as date) as period_start,
               try_cast(period_end as date) as period_end,
               cast(value as double) as value, fiscal_year, fiscal_period, form,
               cast(filed as date) as filed, accession, source_file,
               cast(loaded_at as timestamp) as loaded_at
        from f_df""")


def build(prices, fundamentals):
    con = duckdb.connect()
    con.execute("create macro cast_date(x) as cast(x as date)")
    load_fixtures(con, prices, fundamentals)
    paths = {p.stem: p for p in MODELS.glob("**/*.sql")}
    for name in model_order(paths):
        try:
            con.execute(f"create or replace table {name} as {translate(paths[name].read_text())}")
        except Exception as exc:
            failures.append(f"{name} failed to build: {exc}")
            raise SystemExit(report())
    return con


def check(con, name: str, sql: str, warn: bool = False) -> None:
    """A check passes when its query returns no rows."""
    try:
        rows = con.execute(f"select count(*) from ({sql})").fetchone()[0]
    except Exception as exc:
        failures.append(f"{name}: query failed: {exc}")
        return
    if not rows:
        return
    sample = con.execute(f"select * from ({sql}) limit 3").df().to_dict("records")
    (warnings if warn else failures).append(f"{name}: {rows} row(s), e.g. {sample}")


def expect(name: str, got, want, tol: float = 0.0) -> None:
    if isinstance(want, float) and isinstance(got, (int, float)):
        ok = abs(got - want) <= tol * abs(want)
    else:
        ok = got == want
    if not ok:
        failures.append(f"{name}: got {got!r}, expected {want!r}")


def yml_tests(con) -> None:
    """Run the generic tests declared in the models' .yml files."""
    for path in MODELS.glob("**/*.yml"):
        doc = yaml.safe_load(path.read_text()) or {}
        for model in doc.get("models", []):
            table = model["name"]
            for col in model.get("columns", []):
                for test in col.get("tests", []) or []:
                    run_generic(con, table, col["name"], test)
            for test in model.get("tests", []) or []:
                run_generic(con, table, None, test)


def run_generic(con, table: str, column, test) -> None:
    kind, cfg = (test, {}) if isinstance(test, str) else next(iter(test.items()))
    cfg = cfg or {}
    config = cfg.get("config", {}) if isinstance(cfg, dict) else {}
    # dbt 1.12 wants generic test arguments nested under `arguments`
    cfg = cfg.get("arguments", cfg) if isinstance(cfg, dict) else cfg
    where = config.get("where")
    warn = config.get("severity") == "warn"
    guard = f" and ({where})" if where else ""
    label = f"{table}.{column or '*'} {kind}"

    if kind == "not_null":
        check(con, label, f"select * from {table} where {column} is null", warn)
    elif kind == "unique":
        check(con, label,
              f"select {column} from {table} where {column} is not null "
              f"group by 1 having count(*) > 1", warn)
    elif kind == "accepted_values":
        vals = ", ".join("'" + str(v) + "'" for v in cfg["values"])
        check(con, label,
              f"select * from {table} where {column} is not null "
              f"and {column} not in ({vals}){guard}", warn)
    elif kind == "relationships":
        parent = re.search(r"ref\('(\w+)'\)", cfg["to"]).group(1)
        check(con, label,
              f"select * from {table} c where {column} is not null and not exists "
              f"(select 1 from {parent} p where p.{cfg['field']} = c.{column})", warn)
    elif kind == "dbt_utils.accepted_range":
        bounds = []
        inclusive = cfg.get("inclusive", True)
        if "min_value" in cfg:
            bounds.append(f"{column} <{'' if inclusive else '='} {cfg['min_value']}")
        if "max_value" in cfg:
            bounds.append(f"{column} >{'' if inclusive else '='} {cfg['max_value']}")
        check(con, label,
              f"select * from {table} where {column} is not null "
              f"and ({' or '.join(bounds)}){guard}", warn)
    elif kind == "dbt_utils.unique_combination_of_columns":
        cols = ", ".join(cfg["combination_of_columns"])
        check(con, f"{table} unique ({cols})",
              f"select {cols} from {table} group by {cols} having count(*) > 1", warn)
    else:
        failures.append(f"{label}: no runner for test type {kind!r}")


KNOWN_REVENUE = {   # headline revenue for the mid-2025 quarter, $bn
    "AAPL": ("2025-06-28", 94.036), "MSFT": ("2025-06-30", 76.441),
    "GOOGL": ("2025-06-30", 96.428), "NVDA": ("2025-07-27", 46.743),
    "MA": ("2025-06-30", 8.133), "XOM": ("2025-06-30", 81.506),
    "IBM": ("2025-06-30", 16.977), "GE": ("2025-06-30", 11.023),
    "COST": ("2025-05-11", 63.205), "MCD": ("2025-06-30", 6.843),
}


def value_checks(con) -> None:
    def one(sql):
        return con.execute(sql).fetchone()

    for ticker, (period, revenue_bn) in KNOWN_REVENUE.items():
        got = one(f"""select revenue / 1e9 from int_fundamentals_quarterly
                      where ticker = '{ticker}' and period_end = date '{period}'""")
        if got is None or got[0] is None:
            failures.append(f"revenue {ticker} {period}: missing")
        else:
            expect(f"revenue {ticker} {period}", round(got[0], 3), revenue_bn, tol=0.001)

    # The as-of join: the June 2025 quarter, not the June 2024 one.
    got = one("""select fundamentals_period_end from fct_daily_metrics
                 where ticker = 'AAPL' and trade_date = date '2025-09-02'""")
    expect("AAPL quarter attached on 2025-09-02", str(got[0]), "2025-06-28")

    # NFLX 10-for-1: price drops 10x, share count rises 10x, market cap doesn't move.
    caps = con.execute("""select market_cap from fct_daily_metrics
        where ticker = 'NFLX' and trade_date in (date '2025-11-14', date '2025-11-17')
        order by trade_date""").df()["market_cap"]
    before, after = float(caps.iloc[0]), float(caps.iloc[1])
    if not 0.8 < after / before < 1.25:
        failures.append(f"NFLX market cap jumped across its split: {before:.3g} -> {after:.3g}")
    if not 3e11 < after < 7e11:
        failures.append(f"NFLX market cap after the split looks wrong: {after:.3g}")

    # HON's 0.9535 event was a 1-for-2 reverse split plus a spinoff.
    got = one("""select share_split_ratio from int_stock_splits
                 where ticker = 'HON' and split_date = date '2026-06-29'""")
    expect("HON reverse split ratio", round(got[0], 3), 0.5)
    got = one("""select share_split_ratio from int_stock_splits
                 where ticker = 'HON' and split_date = date '2025-10-30'""")
    expect("HON spinoff leaves the share count alone", got[0], None)

    latest = "(select max(trade_date) from fct_daily_metrics)"

    # MCD files shares in millions; the cover page count rescues it.
    shares, cap = one(f"""select shares_outstanding, market_cap from fct_daily_metrics
                          where ticker = 'MCD' and trade_date = {latest}""")
    if shares is None or not 6e8 < shares < 8e8:
        failures.append(f"MCD share count looks wrong: {shares}")
    if cap is None or not 1e11 < cap < 3e11:
        failures.append(f"MCD market cap looks wrong: {cap}")

    # MA and UPS put one share class on the cover page, V has no count at all.
    for ticker, low, high in [("MA", 8e8, 1.1e9), ("UPS", 7.5e8, 1e9)]:
        basis, shares = one(f"""select shares_basis, shares_outstanding from fct_daily_metrics
                                where ticker = '{ticker}' and trade_date = {latest}""")
        if basis == "cover_page":
            failures.append(f"{ticker} used its cover page count, which is one share class")
        if shares is None or not low < shares < high:
            failures.append(f"{ticker} share count looks wrong: {shares}")
    got = one(f"select market_cap from fct_daily_metrics where ticker = 'V' and trade_date = {latest}")
    expect("V has no market cap (SEC only reports its classes separately)", got[0], None)

    # 16-week fourth quarters still produce a TTM.
    for ticker in ["COST", "PEP"]:
        got = one(f"""select pe_ratio, price_to_sales from fct_daily_metrics
                      where ticker = '{ticker}' and trade_date = {latest}""")
        if got[0] is None or got[1] is None:
            failures.append(f"{ticker} lost its P/E or P/S: {got}")

    # GE's mis-tagged $2.6bn Q4s used to show up as ~977% QoQ growth.
    check(con, "GE quarterly revenue 2015-2017 is plausible",
          """select period_end, revenue from int_fundamentals_quarterly
             where ticker = 'GE' and period_end between date '2015-01-01' and date '2017-12-31'
               and (revenue < 20e9 or revenue > 45e9)""")

    # IBM's Q4 2021 across the Kyndryl spinoff is dropped rather than kept at $3.3bn.
    got = one("""select revenue from int_fundamentals_quarterly
                 where ticker = 'IBM' and period_end = date '2021-12-31'""")
    expect("IBM Q4 2021 revenue dropped", got[0], None)

    # Predecessor CIKs: XOM's history would otherwise start in 2026.
    got = one("select count(*) from fct_daily_metrics where ticker = 'XOM' and market_cap is not null")
    total = one("select count(distinct trade_date) from fct_daily_metrics")[0]
    if got[0] < total * 0.95:
        failures.append(f"XOM has a market cap on only {got[0]} of {total} days")

    # The sector mart has to aggregate the ratios, not average them: total
    # market cap over total TTM earnings, loss makers included.
    check(con, "sector cap-weighted P/E reconciles to the fact table",
          """select m.sector, m.cap_weighted_pe, f.expected
             from mart_sector_daily m
             join (
                 select trade_date, sector,
                        sum(case when ttm_net_income is not null then market_cap end)
                            / nullif(sum(case when market_cap is not null then ttm_net_income end), 0)
                            as expected
                 from fct_daily_metrics group by 1, 2
             ) f using (trade_date, sector)
             where abs(coalesce(m.cap_weighted_pe, 0) - coalesce(f.expected, 0)) > 1e-6""")

    # Sector totals should say when they're missing someone (V).
    got = one(f"""select companies_missing_market_cap from mart_sector_daily
                  where sector = 'Financials' and trade_date = {latest}""")
    expect("Financials flags its missing company", got[0], 1)


def invariance_check(prices, fundamentals) -> None:
    """Daily incremental loads and one big backfill must give the same prices.

    Yahoo adjusts history for splits as of the download, so the same day
    downloaded before and after a split arrives on a different basis.
    """
    events = prices[prices.stock_splits > 0][["ticker", "date", "stock_splits"]]
    incremental = prices.copy()
    fetch = pd.to_datetime(incremental["date"]) + pd.Timedelta(days=1)
    factor = pd.Series(1.0, index=incremental.index)
    for _, e in events.iterrows():
        mask = ((incremental.ticker == e.ticker)
                & (pd.to_datetime(incremental.date) < pd.Timestamp(e.date))
                & (pd.Timestamp(e.date) >= fetch))
        factor[mask] *= e.stock_splits
    for col in ["open", "high", "low", "close", "adj_close", "dividends"]:
        incremental[col] = incremental[col] * factor
    incremental["volume"] = incremental["volume"] / factor
    incremental["fetched_at"] = fetch.dt.strftime("%Y-%m-%d") + " 10:00:00"

    query = """select ticker, trade_date, close_price, split_adjusted_close_price, daily_return
               from int_prices_daily order by 1, 2"""
    a = build(prices, fundamentals).execute(query).df()
    b = build(incremental, fundamentals).execute(query).df()
    if len(a) != len(b):
        failures.append(f"load pattern changed the row count: {len(a)} vs {len(b)}")
        return
    for col in ["close_price", "split_adjusted_close_price", "daily_return"]:
        diff = (a[col] / b[col] - 1).abs().max()
        if pd.notna(diff) and diff > 1e-9:
            failures.append(f"load pattern changed {col} by {diff:.2e}")


def synthetic_regressions(prices, fundamentals) -> None:
    """Cases the real fixture doesn't happen to contain.

    Some guards only bite on data that isn't in the sample right now: no
    company in it currently files a cover count for one share class, and
    nothing tags an old period years late. Without these, reverting those
    fixes leaves every test green.
    """
    # A fact for an old period, first tagged years later. The as-of join has
    # to keep using the newest quarter, not jump back to 2019 on the day the
    # old number appears. Ordering the join by filed date instead of period
    # end brings this bug back.
    late = fundamentals.iloc[0].copy()
    late.update({"ticker": "AAPL", "cik": "0000320193", "metric": "revenue", "tag": "Revenues",
                 "unit": "USD", "period_start": "2019-01-02", "period_end": "2019-04-01",
                 "value": 53.8e9, "fiscal_year": 2026, "fiscal_period": "Q2", "form": "10-Q",
                 "filed": pd.Timestamp("2026-03-02"), "accession": "synthetic-late-tag"})
    with_late = pd.concat([fundamentals, pd.DataFrame([late])], ignore_index=True)
    con = build(prices, with_late)
    got = con.execute("""select fundamentals_period_end from fct_daily_metrics
                         where ticker = 'AAPL' and trade_date = date '2026-03-03'""").fetchone()
    expect("late-tagged 2019 fact doesn't become AAPL's current quarter", str(got[0]), "2025-12-27")

    # A cover page count covering one share class, filed with a 10-Q whose
    # us-gaap count says otherwise. MA's real cover count is 122.5M against
    # ~883M shares; using it divides market cap by 7.
    bad_cover = fundamentals.iloc[0].copy()
    bad_cover.update({"ticker": "MA", "cik": "0001141391", "metric": "cover_shares_outstanding",
                      "tag": "EntityCommonStockSharesOutstanding", "unit": "shares",
                      "period_start": None, "period_end": "2026-06-30", "value": 122_530_193.0,
                      "fiscal_year": 2026, "fiscal_period": "Q2", "form": "10-Q",
                      "filed": pd.Timestamp("2026-07-30"), "accession": "synthetic-class-a-cover"})
    with_bad_cover = pd.concat([fundamentals, pd.DataFrame([bad_cover])], ignore_index=True)
    con = build(prices, with_bad_cover)
    basis, shares = con.execute("""select shares_basis, shares_outstanding from fct_daily_metrics
        where ticker = 'MA' and trade_date = (select max(trade_date) from fct_daily_metrics)""").fetchone()
    if basis == "cover_page" or shares is None or shares < 8e8:
        failures.append(f"one-class cover count was used for MA: {basis}, {shares}")

    # A company missing a quarter, so four TTM rows cover fifteen months.
    # P/E has to go null rather than quietly annualise the wrong span. The
    # gap is AAPL's March 2025 quarter, which nothing else is derived from.
    gapped = fundamentals[~((fundamentals.ticker == "AAPL")
                            & (fundamentals.period_end == "2025-03-29"))]
    con = build(prices, gapped)
    check(con, "no P/E from a TTM spanning more than four quarters",
          """select ticker, trade_date, fundamentals_period_end, pe_ratio
             from fct_daily_metrics
             where ticker = 'AAPL'
               and fundamentals_period_end in
                   (date '2025-06-28', date '2025-09-27', date '2025-12-27')
               and pe_ratio is not null""")

    # The same day loaded twice, the older copy carrying junk prices. Whatever
    # was fetched most recently should win.
    day = prices.trade_date.max() if "trade_date" in prices else prices.date.max()
    stale = prices[prices.date == day].copy()
    for col in ["open", "high", "low", "close", "adj_close"]:
        stale[col] = stale[col] * 3
    stale["fetched_at"] = "2026-01-01 10:00:00"
    stale["loaded_at"] = "2026-01-01 12:00:00"
    con = build(pd.concat([prices, stale], ignore_index=True), fundamentals)
    check(con, "a reloaded partition keeps the newest copy",
          f"""select p.ticker, p.close_price from int_prices_daily p
              where p.trade_date = date '{day}'
                and p.close_price > (select 1.5 * min(close) from raw_prices r
                                     where r.ticker = p.ticker and r.trade_date = date '{day}')""")


def report() -> int:
    for w in warnings:
        print(f"WARN  {w}")
    for f in failures:
        print(f"FAIL  {f}")
    print(f"\n{len(failures)} failed, {len(warnings)} warnings")
    return 1 if failures else 0


def main() -> int:
    prices = pd.read_parquet(FIXTURES / "prices.parquet").assign(
        fetched_at="2026-09-11 10:00:00", loaded_at="2026-09-11 12:00:00")
    fundamentals = pd.read_parquet(FIXTURES / "fundamentals.parquet").assign(
        source_file="raw/fundamentals/snapshot_date=2026-09-11/fundamentals.parquet",
        loaded_at="2026-09-11 12:00:00")

    con = build(prices, fundamentals)
    print(f"built {len(list(MODELS.glob('**/*.sql')))} models on "
          f"{fundamentals.ticker.nunique()} tickers")

    yml_tests(con)
    for path in sorted(SINGULAR_TESTS.glob("*.sql")):
        sql = path.read_text()
        check(con, path.stem, translate(sql), warn="severity='warn'" in sql)
    value_checks(con)
    invariance_check(prices, fundamentals)
    synthetic_regressions(prices, fundamentals)
    return report()


if __name__ == "__main__":
    sys.exit(main())
