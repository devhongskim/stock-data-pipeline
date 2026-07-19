import os
import logging
import duckdb
import boto3
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
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

def generate_gold_metrics(silver_path, yesterday):
    local_silver_file = silver_path
    local_gold_file = f"temp_gold_{yesterday}.parquet"
    
    logger.info(f"Starting Gold Metrics generation for: {yesterday}")
    
    try:
        # Load Silver from the local file
        df_silver = pd.read_parquet(local_silver_file)
        
        # Calculate Gold metrics using DuckDB
        with duckdb.connect() as con:
            df_gold = con.execute("""
                SELECT 
                    trading_date, 
                    ticker, 
                    ROUND(((close_price - open_price) / open_price) * 100, 2) as daily_return,
                    ROUND(((high_price - low_price) / open_price) * 100, 2) as intraday_volatility
                FROM df_silver
            """).df()
            
        # Load to Postgres and S3
        load_gold_to_postgres(df_gold)
        upload_gold_to_s3(df_gold, yesterday, os.getenv("AWS_BRONZE_BUCKET"), local_gold_file)
        
        logger.info(f"🎉 Gold layer finalized for {yesterday}")
        return True

    except Exception as e:
        logger.error(f"Analytics/Gold generation failed for {yesterday}: {e}")
        raise e
        
    finally:
        # Cleanup temporary files 
        if os.path.exists(local_silver_file):
            os.remove(local_silver_file)
        if os.path.exists(local_gold_file): 
            os.remove(local_gold_file)
            

def upload_gold_to_s3(df_gold, yesterday, bucket_name, local_gold_file):
    df_gold.to_parquet(local_gold_file)
    s3_path = f"gold/stocks/date={yesterday}/stocks_metrics_{yesterday}.parquet"
    
    try:
        s3_client.upload_file(local_gold_file, bucket_name, s3_path)
        logger.info(f"Successfully uploaded Gold data to S3: {s3_path}")
    except Exception as e:
        logger.error(f"Error uploading to S3: {e}")
        raise e

def load_gold_to_postgres(df_gold):
    # Check if DB credentials exist before trying to connect
    db_host = os.getenv("DB_HOST")
    if not db_host:
        logger.info("Database credentials not found. Skipping Postgres load (running in Cloud mode).")
        return
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"), 
        database=os.getenv("DB_NAME"), 
        user=os.getenv("DB_USER"), 
        password=os.getenv("DB_PASSWORD"), 
        port=os.getenv("DB_PORT")
    )
    try:
        with conn.cursor() as cur:
            metric_data = [tuple(x) for x in df_gold[['trading_date', 'ticker', 'daily_return', 'intraday_volatility']].to_numpy()]
            execute_values(cur, """
                INSERT INTO market_metrics (trading_date, ticker, daily_return, intraday_volatility)
                VALUES %s
                ON CONFLICT (ticker, trading_date) DO UPDATE SET 
                    daily_return = EXCLUDED.daily_return, intraday_volatility = EXCLUDED.intraday_volatility;
            """, metric_data)
            conn.commit()
            logger.info("Successfully loaded Gold metrics into Postgres.")
    finally:
        conn.close()
