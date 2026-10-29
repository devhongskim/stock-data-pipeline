-- Registers this pipeline's Gold output as an Athena table, queryable in place
-- (no data copy), using partition projection on the existing date= layout:
--   s3://hongskim-stock-pipeline/gold/stocks/date=YYYY-MM-DD/stocks_metrics_YYYY-MM-DD.parquet
--
-- Notes:
-- * `date` is a reserved word in Athena: backticks in DDL, double quotes in queries.
-- * trading_date is intentionally omitted (pandas may write it as a timestamp
--   rather than a date); the `date` partition carries the same value.
-- * Partition projection means no MSCK REPAIR TABLE: each new day the DAG writes
--   is queryable immediately.
-- * Keep comments OUTSIDE the CREATE statement: Athena's DDL parser fails on
--   inline -- comments inside the column list.
--
-- Used by the batch + streaming join in the companion streaming project:
-- https://github.com/devhongskim/realtime-crypto-aggregator

CREATE EXTERNAL TABLE IF NOT EXISTS stock_gold_metrics (
    ticker STRING,
    daily_return DOUBLE COMMENT 'open-to-close % change',
    intraday_volatility DOUBLE COMMENT '(high - low) / open, %'
)
PARTITIONED BY (`date` STRING)
STORED AS PARQUET
LOCATION 's3://hongskim-stock-pipeline/gold/stocks/'
TBLPROPERTIES (
    'projection.enabled'        = 'true',
    'projection.date.type'      = 'date',
    'projection.date.format'    = 'yyyy-MM-dd',
    'projection.date.range'     = '2026-07-01,NOW',
    'storage.location.template' = 's3://hongskim-stock-pipeline/gold/stocks/date=${date}/'
);

-- Sanity check (trading days per ticker; a complete ticker has the same count as the others):
-- SELECT ticker, count(*) AS trading_days
-- FROM stock_gold_metrics
-- WHERE "date" BETWEEN '2026-07-01' AND '2026-08-07'
--   AND daily_return IS NOT NULL
-- GROUP BY ticker
-- ORDER BY trading_days, ticker;
