-- Snowflake setup for the market data pipeline.
--
-- Run it section by section in Snowsight, not all at once. After step 3,
-- copy the DESC INTEGRATION output into terraform.tfvars, re-apply, then
-- carry on from step 4.


-- 1. Warehouse, database, schemas

USE ROLE ACCOUNTADMIN;

-- XSMALL is plenty. AUTO_SUSPEND = 60 so an idle warehouse doesn't burn
-- trial credits.
CREATE WAREHOUSE IF NOT EXISTS MARKET_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Compute for market data pipeline';

CREATE DATABASE IF NOT EXISTS MARKET_DATA
  COMMENT = 'Market prices and SEC fundamentals';

-- RAW is loaded data, never modified. dbt builds everything else.
CREATE SCHEMA IF NOT EXISTS MARKET_DATA.RAW
  COMMENT = 'Landing zone, loaded from S3, never transformed in place';

CREATE SCHEMA IF NOT EXISTS MARKET_DATA.ANALYTICS
  COMMENT = 'dbt-managed models';


-- 2. File format

CREATE FILE FORMAT IF NOT EXISTS MARKET_DATA.RAW.PARQUET_FORMAT
  TYPE = PARQUET
  COMPRESSION = SNAPPY;


-- 3. Storage integration
--
-- Role ARN comes from `terraform output snowflake_role_arn`.
-- STORAGE_ALLOWED_LOCATIONS keeps Snowflake to the raw/ prefix.

CREATE STORAGE INTEGRATION IF NOT EXISTS S3_MARKET_INT
  CREATE STORAGE INTEGRATION IF NOT EXISTS S3_MARKET_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::742031403615:role/market-data-pipeline-snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://market-data-pipeline-raw-66c79357/raw/');

-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID into
-- terraform.tfvars (snowflake_iam_user_arn, snowflake_external_id), run
-- terraform apply, then continue.
DESC INTEGRATION S3_MARKET_INT;


-- 4. External stages (after the second terraform apply)

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

-- If these list files, the trust policy, integration and stages all work.
LIST @PRICES_STAGE;
LIST @FUNDAMENTALS_STAGE;


-- 5. Target tables
--
-- trade_date comes from the S3 path (date=YYYY-MM-DD) since the price
-- files don't contain it.

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


-- 6. Load prices
--
-- COPY INTO skips files it has already loaded. A file overwritten in S3
-- has a new checksum though, so it loads again.

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


-- 7. Load fundamentals
--
-- period_start is empty for point-in-time values, TRY_TO_DATE leaves those null.

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


-- 8. Verify

SELECT COUNT(*) AS row_count,
       COUNT(DISTINCT ticker) AS tickers,
       MIN(trade_date) AS earliest,
       MAX(trade_date) AS latest
FROM MARKET_DATA.RAW.PRICES;

SELECT COUNT(*) AS row_count,
       COUNT(DISTINCT ticker) AS tickers,
       COUNT(DISTINCT metric) AS metrics
FROM MARKET_DATA.RAW.FUNDAMENTALS;

-- nulls here mean the path regex didn't match
SELECT COUNT(*) AS null_dates
FROM MARKET_DATA.RAW.PRICES
WHERE trade_date IS NULL;
