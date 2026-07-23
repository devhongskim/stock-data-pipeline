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
    schedule='30 5 * * 2-6',
    start_date=datetime(2026, 7, 20),
    catchup=False,
    tags=['finance', 'stock_data', 'medallion'],
)
def stock_market_pipeline():

    @task
    def check_market_calendar(**context):
        """1. GATEKEEPER: Check Market Schedule matching main.py logic (previous day offset)"""
        # Use Airflow's logical execution date (ds), falling back to yesterday if run ad-hoc
        logical_ds = context.get('ds')
        if logical_ds:
            base_date = datetime.strptime(logical_ds, '%Y-%m-%d')
        else:
            base_date = datetime.now()
            
        # Match main.py offset behavior to evaluate the correct target session
        target_date_obj = base_date - timedelta(days=1)
        target_date = target_date_obj.strftime('%Y-%m-%d')
        #target_date = "2026-07-17"  # Hardcoded for testing
        
        nyse = mcal.get_calendar('NYSE')
        valid_days = nyse.valid_days(start_date=target_date, end_date=target_date)
        
        if len(valid_days) == 0:
            print(f"🛑 Central Optimization Gate: {target_date} was a weekend or holiday. Skipping.")
            return None
        
        print(f"🟢 Market was OPEN on {target_date}. Proceeding...")
        return target_date

    @task
    def run_extraction(target_date):
        if not target_date:
            return None
        
        bronze_key = fetch_stock_data(target_date, force_overwrite=False) # Keep True for testing. False in production.
        
        if not bronze_key:
            raise ValueError("🛑 Bronze Extraction failed.")
                
        return bronze_key

    @task
    def run_transformation(bronze_key, target_date):
        """3. TRANSFORMATION PHASE: Calls your transform_bronze_to_silver"""
        if not bronze_key or not target_date:
            return None
            
        silver_key = transform_bronze_to_silver(bronze_key, target_date)
        if not silver_key:
            raise ValueError("🛑 Silver Transformation failed.")
        return silver_key

    @task
    def run_analytics(silver_key, target_date):
        """4. ANALYTICS PHASE: Calls your generate_gold_metrics"""
        if not silver_key or not target_date:
            return False
            
        # Calls your exact function from analytics.py
        gold_success = generate_gold_metrics(silver_key, target_date)
        if not gold_success:
            raise ValueError("🛑 Gold Metrics generation failed.")
        return True

    # --- Workflow Dependencies Wiring ---
    market_date = check_market_calendar()
    bronze_file = run_extraction(market_date)
    silver_file = run_transformation(bronze_file, market_date)
    run_analytics(silver_file, market_date)

stock_market_pipeline()