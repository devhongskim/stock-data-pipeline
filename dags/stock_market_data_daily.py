from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas_market_calendars as mcal
from airflow.exceptions import AirflowSkipException

# Import your actual pipeline functions from your modules
from extract import fetch_stock_data
from transform import transform_bronze_to_silver
from analytics import generate_gold_metrics

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
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
        """GATEKEEPER: Check Market Schedule matching main.py logic (previous day offset)"""
        # Use Airflow's logical execution date (ds), falling back to yesterday if run ad-hoc
        logical_ds = context.get('ds')
        if logical_ds:
            base_date = datetime.strptime(logical_ds, '%Y-%m-%d')
        else:
            base_date = datetime.now()
            
        target_date_obj = base_date - timedelta(days=1)
        target_date = target_date_obj.strftime('%Y-%m-%d')
        #target_date = "2026-07-17"  # Hardcoded for testing
        
        nyse = mcal.get_calendar('NYSE')
        valid_days = nyse.valid_days(start_date=target_date, end_date=target_date)
        
        if len(valid_days) == 0:
            raise AirflowSkipException(f"🛑 {target_date} was a weekend or holiday.")
        
        print(f"🟢 Market was OPEN on {target_date}. Proceeding...")
        return target_date

    @task
    def run_extraction(target_date):
        return fetch_stock_data(target_date, force_overwrite=False) # Keep True for testing. False in production.

    @task
    def run_transformation(bronze_key, target_date):
        return transform_bronze_to_silver(bronze_key, target_date)

    @task
    def run_analytics(silver_key, target_date):
        generate_gold_metrics(silver_key, target_date)

    # --- Workflow Dependencies Wiring ---
    market_date = check_market_calendar()
    bronze_file = run_extraction(market_date)
    silver_file = run_transformation(bronze_file, market_date)
    run_analytics(silver_file, market_date)

stock_market_pipeline()