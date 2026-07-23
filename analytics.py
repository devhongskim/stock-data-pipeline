import os
import logging
import duckdb
import boto3
import pandas as pd
from dotenv import load_dotenv

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

def generate_gold_metrics(silver_s3_key, yesterday):
    bucket_name = os.getenv("AWS_BUCKET")
    local_silver_file = f"temp_silver_{yesterday}.parquet"
    local_gold_file = f"temp_gold_{yesterday}.parquet"
    local_duckdb_file = "stock_metrics.duckdb"
    duckdb_s3_key = "data/stock_metrics.duckdb"
    
    logger.info(f"Starting Gold Metrics generation for: {yesterday}")
    
    try:
        # 1. Download persistent DuckDB file from S3 if it exists
        try:
            s3_client.download_file(bucket_name, duckdb_s3_key, local_duckdb_file)
            logger.info("Successfully downloaded existing stock_metrics.duckdb from S3.")
        except Exception:
            logger.info("No existing stock_metrics.duckdb found in S3. A new one will be created.")

        # 2. Connect to DuckDB Metrics Warehouse
        con = duckdb.connect(local_duckdb_file)
        
        # 3. Download Silver Data
        s3_client.download_file(bucket_name, silver_s3_key, local_silver_file)
        df_silver = pd.read_parquet(local_silver_file)


        try:
            # Register pandas dataframe so DuckDB can query it directly
            con.register("df_silver", df_silver)

            # Ensure gold metrics table exists with proper primary key for upserts
            con.execute("""
                CREATE TABLE IF NOT EXISTS market_metrics (
                    trading_date DATE,
                    ticker VARCHAR,
                    daily_return DOUBLE,
                    intraday_volatility DOUBLE,
                    PRIMARY KEY (ticker, trading_date)
                )
            """)

            # Calculate and upsert metrics directly into DuckDB
            con.execute("""
                INSERT INTO market_metrics (
                trading_date, 
                ticker, 
                daily_return, 
                intraday_volatility)
                SELECT 
                    trading_date,
                    ticker,
                    ROUND(((close_price - open_price) / open_price) * 100, 2) as daily_return,
                    ROUND(((high_price - low_price) / open_price) * 100, 2) as intraday_volatility
                FROM df_silver
                ON CONFLICT (ticker, trading_date) DO UPDATE SET 
                    daily_return = EXCLUDED.daily_return, 
                    intraday_volatility = EXCLUDED.intraday_volatility;
            """)        

            logger.info("Successfully loaded and upserted Gold data into DuckDB warehouse.")

        finally:
            con.close()

        # 4. Re-generate df_gold cleanly to push to S3
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
            
        # 5. Load to S3
        upload_gold_to_s3(df_gold, yesterday, bucket_name, local_gold_file)
        s3_client.upload_file(local_duckdb_file, bucket_name, duckdb_s3_key)
        logger.info(f"🎉 Successfully loaded and upserted Gold metrics for {yesterday}")
        return True


    except Exception as e:
        logger.error(f"Gold generation failed for {yesterday}: {e}")
        return False

    # 6. Cleanup temporary files  
    finally:
        if os.path.exists(local_silver_file):
            os.remove(local_silver_file)
        if os.path.exists(local_gold_file): 
            os.remove(local_gold_file)
        if os.path.exists(local_duckdb_file):
            os.remove(local_duckdb_file)
            

def upload_gold_to_s3(df_gold, yesterday, bucket_name, local_gold_file):
    df_gold.to_parquet(local_gold_file)
    s3_path = f"gold/stocks/date={yesterday}/stocks_metrics_{yesterday}.parquet"
    
    try:
        s3_client.upload_file(local_gold_file, bucket_name, s3_path)
        logger.info(f"Successfully uploaded Gold data to S3: {s3_path}")
    except Exception as e:
        logger.error(f"Error uploading to S3: {e}")
        raise e
