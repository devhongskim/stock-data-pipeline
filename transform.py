import os
import logging
import duckdb
import boto3
import psycopg2
from psycopg2.extras import execute_values
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

def transform_bronze_to_silver(local_bronze_file, yesterday):

    bucket_name = os.getenv("AWS_BRONZE_BUCKET")
    local_silver_file = f"temp_silver_{yesterday}.parquet"
    
    logger.info(f"Starting Transformation and Load for: {yesterday}")
    
    try:
        #1 Validate Bronze Data
        df=pd.read_json(local_bronze_file)
        if not validate_bronze_data(df):
            logger.error("🛑 Data Quality Check Failed. Aborting Transformation.")
            return None

        # 2. Transform using DuckDB
        with duckdb.connect() as con:
            df = con.execute(f"""
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
        
        # 3. Load to Postgres
        load_silver_to_postgres(df)
        
        # 4. Upload to S3 Silver and save locally for the next pipeline stage
        df.to_parquet(local_silver_file)
        s3_client.upload_file(local_silver_file, bucket_name, f"silver/stocks/date={yesterday}/stocks_clean_{yesterday}.parquet")
        
        logger.info(f"🥈 Silver layer and Postgres load complete for {yesterday}")
        return local_silver_file

    except Exception as e:
        logger.error(f"Transformation/Load failed for {yesterday}: {e}")
        return None
        
    finally:
        # Only clean up the bronze file. 
        # local_silver_file is kept so analytics.py can access it.
        if os.path.exists(local_bronze_file): 
            os.remove(local_bronze_file)

def load_silver_to_postgres(df):
    # Check if DB credentials exist before trying to connect
    db_host = os.getenv("DB_HOST")
    if not db_host:
        logger.info("Database credentials not found. Skipping Postgres load (running in Cloud mode).")
        return True # Not an error, just skipping Postgres load.
    
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"), 
        database=os.getenv("DB_NAME"), 
        user=os.getenv("DB_USER"), 
        password=os.getenv("DB_PASSWORD"), 
        port=os.getenv("DB_PORT")
    )
    try:
        with conn.cursor() as cur:
            price_data = [tuple(x) for x in df[['trading_date', 'ticker', 'open_price', 'high_price', 'low_price', 'close_price', 'trading_volume', 'after_hours', 'pre_market']].to_numpy()]
            execute_values(cur, """
                INSERT INTO stock_prices (trading_date, ticker, open_price, high_price, low_price, close_price, volume, after_hours, pre_market)
                VALUES %s
                ON CONFLICT (ticker, trading_date) DO UPDATE SET 
                    open_price = EXCLUDED.open_price, high_price = EXCLUDED.high_price, low_price = EXCLUDED.low_price, close_price = EXCLUDED.close_price, volume = EXCLUDED.volume, after_hours = EXCLUDED.after_hours, pre_market = EXCLUDED.pre_market;
            """, price_data)
            conn.commit()
            logger.info("Successfully loaded data into Postgres.")
        return True
    except Exception as e:
        logger.error(f"Postgres load failed: {e}")
        return False
    finally:
        conn.close()