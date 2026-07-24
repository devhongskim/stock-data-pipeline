import sys
import logging
from datetime import datetime, timedelta
import pandas_market_calendars as mcal
from extract import fetch_stock_data
from transform import transform_bronze_to_silver
from analytics import generate_gold_metrics
import argparse

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_pipeline(force_overwrite=False):
    logger.info("🚀 STARTING AUTOMATED CLOUD PIPELINE EXECUTION")
    
    # Calculate yesterday's date
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    #yesterday = "2026-07-17" #hardcode for testing
    
    # 1. GATEKEEPER: Check Market Schedule
    nyse = mcal.get_calendar('NYSE')
    valid_days = nyse.valid_days(start_date=yesterday, end_date=yesterday)
    
    if len(valid_days) == 0:
        logger.info(f"🛑 Central Optimization Gate: {yesterday} was a weekend or holiday. Exiting.")
        return

    logger.info(f"🟢 Market was OPEN on {yesterday}. Initiating processing...")
    
    # 2. STAGE EXECUTION
    try:
        logger.info("--- Starting Stage: Extraction ---")
        bronze_path = fetch_stock_data(yesterday, force_overwrite=force_overwrite)
        
        logger.info("--- Starting Stage: Transformation ---")
        silver_path = transform_bronze_to_silver(bronze_path, yesterday)

        # Analytics Stage
        logger.info("--- Starting Stage: Analytics/Load ---")
        gold_success = generate_gold_metrics(silver_path, yesterday)   
         
    except Exception as e:
        logger.error(f"💥 Pipeline Failure: {e}", exc_info=True)
        sys.exit(1)

    logger.info("🎉 PIPELINE RUN COMPLETE!")

if __name__ == "__main__":
    # 4. Setup Argument Parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--force', action='store_true', help="Force re-download")
    args = parser.parse_args()
    
    # 5. Pass the flag to the runner
    run_pipeline(force_overwrite=args.force)