import os
import logging
import duckdb
import boto3
from dotenv import load_dotenv
from validate import validate_bronze_data
import pandas as pd
from botocore.exceptions import ClientError

# Setup logging
logger = logging.getLogger(__name__)

load_dotenv()

# Constants
S3_REGION = "us-east-1"
BUCKET_NAME = os.getenv("AWS_BUCKET")

def get_verified_s3_client():
    
    # Let boto3 resolve standard environment variables, supplying region explicitly
    client = boto3.client('s3', region_name=S3_REGION)
    
    try:
        client.head_bucket(Bucket=BUCKET_NAME)
        logger.info(f"Successfully verified access to S3 bucket.")
    except ClientError as e:
        raise RuntimeError(f"Pipeline startup aborted: Cannot access S3 bucket. {e}")
        
    return client

def transform_bronze_to_silver(bronze_s3_key, yesterday):
    # Initialize and verify S3 client
    s3_client = get_verified_s3_client()

    local_silver_file = f"temp_silver_{yesterday}.parquet"
    local_bronze_file = f"temp_bronze_{yesterday}.json"
    local_duckdb_file = "stock_raw.duckdb"
    duckdb_s3_key = "data/stock_raw.duckdb"
    silver_s3_key = f"silver/stocks/date={yesterday}/stocks_clean_{yesterday}.parquet"

    logger.info(f"Starting Transformation and Load for: {yesterday}")
    
    try:
        # 1. Download persistent DuckDB file from S3 if it exists
        try:
            s3_client.download_file(BUCKET_NAME, duckdb_s3_key, local_duckdb_file)
            logger.info("Successfully downloaded existing stock_raw.duckdb from S3.")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.info("No existing stock_raw.duckdb found in S3. A new one will be created.")
            else:
                raise RuntimeError(f"🛑 Failed to check/download stock_raw.duckdb: {e}")

        #2 Validate Bronze Data
        s3_client.download_file(BUCKET_NAME, bronze_s3_key, local_bronze_file)

        df_check = pd.read_json(local_bronze_file)
        if not validate_bronze_data(df_check):
            raise RuntimeError("🛑 Data Quality Check Failed. Aborting Transformation.")

        # 3. Transform ONCE with DuckDB, producing a single clean DataFrame
        #    used for both the DuckDB warehouse upsert and the Silver parquet file.
        with duckdb.connect() as con:
            silver_df = con.execute(f"""
                SELECT
                    CAST("from" AS DATE) as trading_date,
                    symbol as ticker,
                    open::DOUBLE as open_price,
                    high::DOUBLE as high_price,
                    low::DOUBLE as low_price,
                    close::DOUBLE as close_price,
                    volume::BIGINT as volume,
                    afterHours::DOUBLE as after_hours,
                    preMarket::DOUBLE as pre_market
                FROM read_json_auto('{local_bronze_file}')
                WHERE status = 'OK'
            """).df()
 
        # 4. Connect to persistent DuckDB Warehouse and upsert the transformed data
        con = duckdb.connect(local_duckdb_file)
        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS stock_prices (
                    trading_date DATE,
                    ticker VARCHAR,
                    open_price DOUBLE,
                    high_price DOUBLE,
                    low_price DOUBLE,
                    close_price DOUBLE,
                    volume BIGINT,
                    after_hours DOUBLE,
                    pre_market DOUBLE,
                    PRIMARY KEY (ticker, trading_date)
                )
            """)
 
            con.register("silver_df", silver_df)
            con.execute("""
                INSERT INTO stock_prices (
                    trading_date, ticker, open_price, high_price,
                    low_price, close_price, volume, after_hours, pre_market
                )
                SELECT
                    trading_date, ticker, open_price, high_price,
                    low_price, close_price, volume, after_hours, pre_market
                FROM silver_df
                ON CONFLICT (ticker, trading_date) DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    after_hours = EXCLUDED.after_hours,
                    pre_market = EXCLUDED.pre_market;
            """)
            logger.info("Successfully loaded and upserted Silver data into DuckDB warehouse.")
        finally:
            con.close()
        
        # 5. Upload parquet to S3 Silver & Backup DuckDB to S3
        silver_df.to_parquet(local_silver_file)
        
        # 6. Upload parquet to S3 Silver & back up DuckDB warehouse to S3
        try:
            s3_client.upload_file(local_silver_file, BUCKET_NAME, f"silver/stocks/date={yesterday}/stocks_clean_{yesterday}.parquet")        
            s3_client.upload_file(local_duckdb_file, BUCKET_NAME, duckdb_s3_key)
        except Exception as e:
            raise RuntimeError(f"🛑 AWS S3 Upload failed for {yesterday}: {e}")

        logger.info(f"🥈 Silver layer and DuckDB load complete for {yesterday}")
        return silver_s3_key

    except Exception as e:
        raise RuntimeError(f"🛑 Transformation/Load failed for {yesterday}: {e}")
        
    finally:
        if os.path.exists(local_bronze_file):
            os.remove(local_bronze_file)
        if os.path.exists(local_silver_file):
            os.remove(local_silver_file)
        if os.path.exists(local_duckdb_file):
            os.remove(local_duckdb_file)