-- ==========================================================================
-- Snowflake setup for the market data pipeline.
--
-- Run these in order. Steps 1-3 first, then update Terraform with the
-- values from step 3, re-apply, then continue from step 4.
--
-- Open a SQL File in Snowsight and paste section by section rather than
-- running the whole file at once; you need to read output partway through.
-- ==========================================================================


-- --------------------------------------------------------------------------
-- 1. Warehouse, database, schemas
-- --------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- XSMALL is the smallest warehouse and plenty for 40k rows. Auto-suspend
-- after 60s of idle is the single most important cost setting on a trial:
-- Snowflake bills per second of warehouse uptime, and a warehouse left
-- running overnight burns credits doing nothing.
CREATE WAREHOUSE IF NOT EXISTS MARKET_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Compute for market data pipeline';

CREATE DATABASE IF NOT EXISTS MARKET_DATA
  COMMENT = 'Market prices and SEC fundamentals';

-- RAW holds data exactly as loaded, untransformed. dbt writes its models
-- into ANALYTICS. Keeping them separate means a bad transformation never
-- destroys the source of truth, and you can always rebuild downstream.
CREATE SCHEMA IF NOT EXISTS MARKET_DATA.RAW
  COMMENT = 'Landing zone, loaded from S3, never transformed in place';

CREATE SCHEMA IF NOT EXISTS MARKET_DATA.ANALYTICS
  COMMENT = 'dbt-managed models';


-- --------------------------------------------------------------------------
-- 2. File format
--
-- Parquet carries its own schema, so unlike CSV there's nothing to
-- configure about delimiters or headers.
-- --------------------------------------------------------------------------
CREATE FILE FORMAT IF NOT EXISTS MARKET_DATA.RAW.PARQUET_FORMAT
  TYPE = PARQUET
  COMPRESSION = SNAPPY;


-- --------------------------------------------------------------------------
-- 3. Storage integration
--
-- Replace <ROLE_ARN> with the `snowflake_role_arn` output from Terraform.
-- Replace <BUCKET> with your bucket name.
--
-- STORAGE_ALLOWED_LOCATIONS restricts this integration to one prefix.
-- Even if the IAM role were over-permissive, Snowflake won't read outside
-- the paths listed here. Defence in depth.
-- --------------------------------------------------------------------------
CREATE STORAGE INTEGRATION IF NOT EXISTS S3_MARKET_INT
  CREATE STORAGE INTEGRATION IF NOT EXISTS S3_MARKET_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::742031403615:role/market-data-pipeline-snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://market-data-pipeline-raw-66c79357/raw/');

-- Run this and copy the two values out. They're what AWS needs in the
-- role's trust policy.
--
--   STORAGE_AWS_IAM_USER_ARN   -> snowflake_iam_user_arn in tfvars
--   STORAGE_AWS_EXTERNAL_ID    -> snowflake_external_id in tfvars
--
-- STOP HERE. Update terraform.tfvars, run terraform apply, then continue.
DESC INTEGRATION S3_MARKET_INT;


-- --------------------------------------------------------------------------
-- 4. External stages  (only after the Terraform re-apply)
--
-- A stage is a named pointer to a location, so queries reference
-- @STAGE_NAME instead of repeating bucket paths and credentials.
-- --------------------------------------------------------------------------
USE DATABASE MARKET_DATA;
USE SCHEMA RAW;

CREATE STAGE IF NOT EXISTS PRICES_STAGE
  STORAGE_INTEGRATION = S3_MARKET_INT
  URL = 's3://<BUCKET>/raw/prices/'
  FILE_FORMAT = MARKET_DATA.RAW.PARQUET_FORMAT;

CREATE STAGE IF NOT EXISTS FUNDAMENTALS_STAGE
  STORAGE_INTEGRATION = S3_MARKET_INT
  URL = 's3://<BUCKET>/raw/fundamentals/'
  FILE_FORMAT = MARKET_DATA.RAW.PARQUET_FORMAT;

-- Verify the whole chain: IAM trust policy, integration, stage.
-- If this lists files, the cross-account access works. If it errors,
-- the trust policy is wrong and no amount of COPY INTO tuning will help.
LIST @PRICES_STAGE;
LIST @FUNDAMENTALS_STAGE;


-- --------------------------------------------------------------------------
-- 5. Target tables
--
-- Loaded columns are explicit rather than using a VARIANT blob. Typed
-- columns give you real constraints and much better query performance.
--
-- The date column is populated from the S3 path, not from file contents:
-- the ingestion writes date=YYYY-MM-DD/ partitions and drops the column
-- from the file itself, so it has to be recovered from METADATA$FILENAME.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MARKET_DATA.RAW.PRICES (
  trade_date    DATE,
  ticker        VARCHAR(16),
  open          FLOAT,
  high          FLOAT,
  low           FLOAT,
  close         FLOAT,
  adj_close     FLOAT,
  volume        FLOAT,
  source_file   VARCHAR(512),
  loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS MARKET_DATA.RAW.FUNDAMENTALS (
  ticker         VARCHAR(16),
  cik            VARCHAR(16),
  metric         VARCHAR(64),
  tag            VARCHAR(256),
  unit           VARCHAR(32),
  period_start   DATE,
  period_end     DATE,
  value          FLOAT,
  fiscal_year    NUMBER(4,0),
  fiscal_period  VARCHAR(8),
  form           VARCHAR(16),
  filed          DATE,
  accession      VARCHAR(64),
  source_file    VARCHAR(512),
  loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- --------------------------------------------------------------------------
-- 6. Load prices
--
-- $1 is the whole Parquet record as a VARIANT; :fieldname pulls a column.
-- Casts are explicit because Parquet's types don't always map to what
-- you want in the table.
--
-- REGEXP_SUBSTR recovers the partition date from the file path. This is
-- exactly what Hive-style partitioning buys: the value lives in the path
-- so it isn't duplicated in every row of the file.
--
-- COPY INTO is idempotent by default: Snowflake tracks which files it has
-- already loaded and skips them. Re-running is safe and won't duplicate.
-- --------------------------------------------------------------------------
COPY INTO MARKET_DATA.RAW.PRICES (
  trade_date, ticker, open, high, low, close, adj_close, volume, source_file
)
FROM (
  SELECT
    TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1, 1, 'e', 1)),
    $1:ticker::VARCHAR,
    $1:open::FLOAT,
    $1:high::FLOAT,
    $1:low::FLOAT,
    $1:close::FLOAT,
    $1:adj_close::FLOAT,
    $1:volume::FLOAT,
    METADATA$FILENAME
  FROM @PRICES_STAGE
)
FILE_FORMAT = (FORMAT_NAME = MARKET_DATA.RAW.PARQUET_FORMAT)
ON_ERROR = 'ABORT_STATEMENT';


-- --------------------------------------------------------------------------
-- 7. Load fundamentals
--
-- period_start is null for balance-sheet metrics (total assets, shares
-- outstanding) because those are point-in-time values, not periods.
-- TRY_TO_DATE returns null instead of erroring on those.
-- --------------------------------------------------------------------------
COPY INTO MARKET_DATA.RAW.FUNDAMENTALS (
  ticker, cik, metric, tag, unit, period_start, period_end,
  value, fiscal_year, fiscal_period, form, filed, accession, source_file
)
FROM (
  SELECT
    $1:ticker::VARCHAR,
    $1:cik::VARCHAR,
    $1:metric::VARCHAR,
    $1:tag::VARCHAR,
    $1:unit::VARCHAR,
    TRY_TO_DATE($1:period_start::VARCHAR),
    TRY_TO_DATE($1:period_end::VARCHAR),
    $1:value::FLOAT,
    $1:fiscal_year::NUMBER,
    $1:fiscal_period::VARCHAR,
    $1:form::VARCHAR,
    TRY_TO_DATE($1:filed::VARCHAR),
    $1:accession::VARCHAR,
    METADATA$FILENAME
  FROM @FUNDAMENTALS_STAGE
)
FILE_FORMAT = (FORMAT_NAME = MARKET_DATA.RAW.PARQUET_FORMAT)
ON_ERROR = 'ABORT_STATEMENT';


-- --------------------------------------------------------------------------
-- 8. Verify
-- --------------------------------------------------------------------------
SELECT COUNT(*) AS row_count,
       COUNT(DISTINCT ticker) AS tickers,
       MIN(trade_date) AS earliest,
       MAX(trade_date) AS latest
FROM MARKET_DATA.RAW.PRICES;

SELECT COUNT(*) AS row_count,
       COUNT(DISTINCT ticker) AS tickers,
       COUNT(DISTINCT metric) AS metrics
FROM MARKET_DATA.RAW.FUNDAMENTALS;

-- Sanity check that partition-date extraction worked. If trade_date is
-- null anywhere, the regex didn't match the file paths.
SELECT COUNT(*) AS null_dates
FROM MARKET_DATA.RAW.PRICES
WHERE trade_date IS NULL;
