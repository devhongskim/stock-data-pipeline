import os
import logging
import duckdb
import boto3
from dotenv import load_dotenv
from validate import validate_bronze_data
import pandas as pd

# Setup logging
logger = logging.getLogger(__name__)

load_dotenv()

# Global S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="us-east-1"
)

def transform_bronze_to_silver(bronze_s3_key, yesterday):

    bucket_name = os.getenv("AWS_BUCKET")
    local_silver_file = f"temp_silver_{yesterday}.parquet"
    local_bronze_file = f"temp_bronze_{yesterday}.json"
    silver_s3_key = f"silver/stocks/date={yesterday}/stocks_clean_{yesterday}.parquet"

    logger.info(f"Starting Transformation and Load for: {yesterday}")
    
    try:
        #1 Validate Bronze Data
        s3_client.download_file(bucket_name, bronze_s3_key, local_bronze_file)

        df=pd.read_json(local_bronze_file)
        if not validate_bronze_data(df):
            logger.error("🛑 Data Quality Check Failed. Aborting Transformation.")
            return None

        # 2. Connect to DuckDB Warehouse and perform combined Transform & Upsert
        con = duckdb.connect("stock_raw.duckdb")

        try:
            # Ensure master table exists with proper primary key for upserts
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

            # Parse, clean, and upsert straight from the JSON source file into DuckDB
            con.execute(f"""
                INSERT INTO stock_prices (
                    trading_date, ticker, open_price, high_price, 
                    low_price, close_price, volume, after_hours, pre_market
                )
                SELECT 
                    CAST("from" AS DATE) as trading_date,
                    symbol as ticker,
                    open::DOUBLE as open_price,
                    high::DOUBLE as high_price,
                    low::DOUBLE as low_price,
                    close::DOUBLE as close_price,
                    volume::BIGINT as trading_volume,
                    afterHours::DOUBLE as after_hours,
                    preMarket::DOUBLE as pre_market
                FROM read_json_auto('{local_bronze_file}')
                WHERE status = 'OK'
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
        
        # 3. Generate Parquet for S3 Silver Layer using an ephemeral DuckDB query
        with duckdb.connect() as con:
            silver_df = con.execute(f"""
                SELECT 
                    CAST("from" AS DATE) as trading_date,
                    symbol as ticker,
                    open::DOUBLE as open_price,
                    high::DOUBLE as high_price,
                    low::DOUBLE as low_price,
                    close::DOUBLE as close_price,
                    volume::BIGINT as trading_volume,
                    afterHours::DOUBLE as after_hours,
                    preMarket::DOUBLE as pre_market
                FROM read_json_auto('{local_bronze_file}')
                WHERE status = 'OK'
            """).df()
        
        # 4. Upload to S3 Silver
        silver_df.to_parquet(local_silver_file)
        s3_client.upload_file(local_silver_file, bucket_name, f"silver/stocks/date={yesterday}/stocks_clean_{yesterday}.parquet")
        
        logger.info(f"🥈 Silver layer and DuckDB load complete for {yesterday}")
        return silver_s3_key

    except Exception as e:
        logger.error(f"Transformation/Load failed for {yesterday}: {e}")
        return None
        
    finally:
        if os.path.exists(local_bronze_file):
            os.remove(local_bronze_file)
        if os.path.exists(local_silver_file):
            os.remove(local_silver_file)