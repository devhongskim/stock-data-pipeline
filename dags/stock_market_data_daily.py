from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas_market_calendars as mcal

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
    schedule='30 5 * * 2-6',  # Matches your cron schedule from GitHub Actions
    start_date=datetime(2026, 7, 20),
    catchup=False,
    tags=['finance', 'stock_data', 'medallion'],
)
def stock_market_pipeline():

    @task
    def check_market_calendar(logical_date=None, **context):
        """1. GATEKEEPER: Check Market Schedule using your exact logic"""
        # In Airflow, we can use the execution date (logical date)
        target_date = context.get('ds') or datetime.now().strftime('%Y-%m-%d')
        
        nyse = mcal.get_calendar('NYSE')
        valid_days = nyse.valid_days(start_date=target_date, end_date=target_date)
        
        if len(valid_days) == 0:
            print(f"🛑 Central Optimization Gate: {target_date} was a weekend or holiday. Skipping.")
            return False
        
        print(f"🟢 Market was OPEN on {target_date}. Proceeding...")
        return target_date

    @task
    def run_extraction(target_date):
        """2. EXTRACTION PHASE: Calls your existing fetch_stock_data"""
        if not target_date:
            return None
        
        # Calls your exact function from extract.py
        bronze_path = fetch_stock_data(target_date, force_overwrite=False)
        if not bronze_path:
            raise ValueError("🛑 Bronze Extraction failed.")
        return bronze_path

    @task
    def run_transformation(bronze_path, target_date):
        """3. TRANSFORMATION PHASE: Calls your transform_bronze_to_silver"""
        if not bronze_path:
            return None
            
        # Calls your exact function from transform.py
        silver_path = transform_bronze_to_silver(bronze_path, target_date)
        if not silver_path:
            raise ValueError("🛑 Silver Transformation failed.")
        return silver_path

    @task
    def run_analytics(silver_path, target_date):
        """4. ANALYTICS PHASE: Calls your generate_gold_metrics"""
        if not silver_path:
            return False
            
        # Calls your exact function from analytics.py
        gold_success = generate_gold_metrics(silver_path, target_date)
        if not gold_success:
            raise ValueError("🛑 Gold Metrics generation failed.")
        return True

    # --- Workflow Dependencies Wiring ---
    market_date = check_market_calendar()
    bronze_file = run_extraction(market_date)
    silver_file = run_transformation(bronze_file, market_date)
    run_analytics(silver_file, market_date)

stock_market_pipeline()