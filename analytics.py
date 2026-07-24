import os
import logging
import duckdb
import boto3
import pandas as pd
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Setup logging
logger = logging.getLogger(__name__)

load_dotenv()

# Constants
S3_REGION = "us-east-1"
BUCKET_NAME = os.getenv("AWS_BUCKET")

def get_verified_s3_client():
    
    client = boto3.client('s3', region_name=S3_REGION)
    
    try:
        client.head_bucket(Bucket=BUCKET_NAME)
        logger.info(f"Successfully verified access to S3 bucket.")
    except ClientError as e:
        raise RuntimeError(f"Pipeline startup aborted: Cannot access S3 bucket. {e}")
        
    return client

def generate_gold_metrics(silver_s3_key, yesterday):
    # Initialize and verify S3 client
    s3_client = get_verified_s3_client()
    
    local_silver_file = f"temp_silver_{yesterday}.parquet"
    local_gold_file = f"temp_gold_{yesterday}.parquet"
    gold_s3_key = f"gold/stocks/date={yesterday}/stocks_metrics_{yesterday}.parquet"
    local_duckdb_file = "stock_metrics.duckdb"
    duckdb_s3_key = "data/stock_metrics.duckdb"
    
    logger.info(f"Starting Gold Metrics generation for: {yesterday}")
    
    try:


        # 1. Download persistent DuckDB file from S3 if it exists
        try:
            s3_client.download_file(BUCKET_NAME, duckdb_s3_key, local_duckdb_file)
            logger.info("Successfully downloaded existing stock_metrics.duckdb from S3.")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.info("No existing stock_metrics.duckdb found in S3. A new one will be created.")
            else:
                raise RuntimeError(f"🛑 Failed to check/download stock_metrics.duckdb: {e}")

        
        # 2. Download Silver Data & Upsert into DuckDB
        s3_client.download_file(BUCKET_NAME, silver_s3_key, local_silver_file)
        df_silver = pd.read_parquet(local_silver_file)

        # 3. Compute Gold metrics ONCE, producing a single DataFrame
        #    used for both the DuckDB warehouse upsert and the Gold parquet file.
        with duckdb.connect() as con:
            con.register("df_silver", df_silver)
            df_gold = con.execute("""
                SELECT
                    trading_date,
                    ticker,
                    ROUND(((close_price - open_price) / open_price) * 100, 2) as daily_return,
                    ROUND(((high_price - low_price) / open_price) * 100, 2) as intraday_volatility
                FROM df_silver
            """).df()
 
        # 4. Connect to persistent DuckDB warehouse and upsert the computed metrics
        con = duckdb.connect(local_duckdb_file)
        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS market_metrics (
                    trading_date DATE,
                    ticker VARCHAR,
                    daily_return DOUBLE,
                    intraday_volatility DOUBLE,
                    PRIMARY KEY (ticker, trading_date)
                )
            """)
 
            con.register("df_gold", df_gold)
            con.execute("""
                INSERT INTO market_metrics (
                    trading_date, ticker, daily_return, intraday_volatility
                )
                SELECT trading_date, ticker, daily_return, intraday_volatility
                FROM df_gold
                ON CONFLICT (ticker, trading_date) DO UPDATE SET
                    daily_return = EXCLUDED.daily_return,
                    intraday_volatility = EXCLUDED.intraday_volatility;
            """)
            logger.info("Successfully loaded and upserted Gold data into DuckDB warehouse.")
        finally:
            con.close()
            
        # 5. Upload parquet to S3 Gold & Backup DuckDB to S3
        df_gold.to_parquet(local_gold_file)

        # 6. Upload parquet to S3 Gold & back up DuckDB warehouse to S3
        try:
            s3_client.upload_file(local_gold_file, BUCKET_NAME, gold_s3_key)
            s3_client.upload_file(local_duckdb_file, BUCKET_NAME, duckdb_s3_key)
        except Exception as e:
            raise RuntimeError(f"🛑 AWS S3 Upload failed for {yesterday}: {e}")

        logger.info(f"🥇 Gold layer and DuckDB load complete for {yesterday}")

    except Exception as e:
        raise RuntimeError(f"🛑 Analytic metrics generation failed for {yesterday}: {e}")
        

    # 7. Cleanup temporary files  
    finally:
        if os.path.exists(local_silver_file):
            os.remove(local_silver_file)
        if os.path.exists(local_gold_file): 
            os.remove(local_gold_file)
        if os.path.exists(local_duckdb_file):
            os.remove(local_duckdb_file)