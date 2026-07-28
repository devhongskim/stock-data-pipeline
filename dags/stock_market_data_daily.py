from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import pandas_market_calendars as mcal

# Import your actual pipeline functions from your modules
from extract import fetch_stock_data, get_verified_s3_client, check_exists, BUCKET_NAME
from transform import transform_bronze_to_silver
from analytics import generate_gold_metrics
from alerts import send_failure_alert

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_failure_alert,
}

@dag(
    dag_id='stock_market_data_daily',
    default_args=default_args,
    schedule='30 5 * * 2-6',
    start_date=datetime(2026, 7, 20),
    catchup=False,
    max_active_runs=1,
    tags=['finance', 'stock_data', 'medallion'],
)
def stock_market_pipeline():

    @task
    def check_market_calendar(**context):
        """1. GATEKEEPER: Check Market Schedule matching main.py logic (previous day offset)"""
        logical_ds = context.get('ds')
        if logical_ds:
            base_date = datetime.strptime(logical_ds, '%Y-%m-%d')
        else:
            base_date = datetime.now()

        target_date_obj = base_date - timedelta(days=1)
        target_date = target_date_obj.strftime('%Y-%m-%d')

        nyse = mcal.get_calendar('NYSE')
        valid_days = nyse.valid_days(start_date=target_date, end_date=target_date)

        if len(valid_days) == 0:
            raise AirflowSkipException(f"🛑 {target_date} was a weekend or holiday.")

        print(f"🟢 Market was OPEN on {target_date}. Proceeding...")
        return target_date

    @task
    def run_extraction(target_date):
        """2. EXTRACTION PHASE: gated on the Bronze layer's own existence."""
        s3_client = get_verified_s3_client()
        bronze_key = f"bronze/stocks/date={target_date}/stocks_{target_date}.json"

        if check_exists(s3_client, BUCKET_NAME, bronze_key):
            print(f"🟡 Bronze layer for {target_date} already exists. Skipping Extraction.")
            return bronze_key

        return fetch_stock_data(target_date, force_overwrite=False)

    @task
    def run_transformation(bronze_key, target_date):
        """3. TRANSFORMATION PHASE: gated on the Silver layer's own existence."""
        s3_client = get_verified_s3_client()
        silver_key = f"silver/stocks/date={target_date}/stocks_clean_{target_date}.parquet"

        if check_exists(s3_client, BUCKET_NAME, silver_key):
            print(f"🟡 Silver layer for {target_date} already exists. Skipping Transformation.")
            return silver_key

        return transform_bronze_to_silver(bronze_key, target_date)

    @task
    def run_analytics(silver_key, target_date):
        """4. ANALYTICS PHASE: gated on the Gold layer's own existence."""
        s3_client = get_verified_s3_client()
        gold_key = f"gold/stocks/date={target_date}/stocks_metrics_{target_date}.parquet"

        if check_exists(s3_client, BUCKET_NAME, gold_key):
            print(f"🟡 Gold layer for {target_date} already exists. Skipping Analytics.")
            return

        generate_gold_metrics(silver_key, target_date)

    # --- Workflow Dependencies Wiring ---
    market_date = check_market_calendar()
    bronze_file = run_extraction(market_date)
    silver_file = run_transformation(bronze_file, market_date)
    run_analytics(silver_file, market_date)

stock_market_pipeline()