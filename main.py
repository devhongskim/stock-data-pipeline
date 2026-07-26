import sys
import logging
from datetime import datetime, timedelta
import pandas_market_calendars as mcal
from extract import fetch_stock_data, get_verified_s3_client, check_exists, BUCKET_NAME
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

    # 1. GATEKEEPER: Check Market Schedule
    nyse = mcal.get_calendar('NYSE')
    valid_days = nyse.valid_days(start_date=yesterday, end_date=yesterday)

    if len(valid_days) == 0:
        logger.info(f"🛑 Central Optimization Gate: {yesterday} was a weekend or holiday. Exiting.")
        return

    logger.info(f"🟢 Market was OPEN on {yesterday}. Initiating processing...")

    # 2. Compute each layer's expected S3 key so we can gate independently on
    #    whether that specific layer's output already exists. This means a
    #    partially-completed or partially-deleted run (e.g. bronze exists but
    #    gold was manually removed) self-heals without any special-casing --
    #    each stage only cares about its own output, not how earlier stages got there.
    bronze_key = f"bronze/stocks/date={yesterday}/stocks_{yesterday}.json"
    silver_key = f"silver/stocks/date={yesterday}/stocks_clean_{yesterday}.parquet"
    gold_key = f"gold/stocks/date={yesterday}/stocks_metrics_{yesterday}.parquet"

    s3_client = get_verified_s3_client()

    # 3. STAGE EXECUTION
    # Each stage raises RuntimeError on failure (rather than returning None/False),
    # so a single try/except here is sufficient to catch and abort on any failure.
    try:
        if force_overwrite or not check_exists(s3_client, BUCKET_NAME, bronze_key):
            logger.info("--- Starting Stage: Extraction ---")
            bronze_path = fetch_stock_data(yesterday, force_overwrite=force_overwrite)
        else:
            logger.info(f"🟡 Bronze layer for {yesterday} already exists. Skipping Extraction.")
            bronze_path = bronze_key

        if force_overwrite or not check_exists(s3_client, BUCKET_NAME, silver_key):
            logger.info("--- Starting Stage: Transformation ---")
            silver_path = transform_bronze_to_silver(bronze_path, yesterday)
        else:
            logger.info(f"🟡 Silver layer for {yesterday} already exists. Skipping Transformation.")
            silver_path = silver_key

        if force_overwrite or not check_exists(s3_client, BUCKET_NAME, gold_key):
            logger.info("--- Starting Stage: Analytics/Load ---")
            generate_gold_metrics(silver_path, yesterday)
        else:
            logger.info(f"🟡 Gold layer for {yesterday} already exists. Skipping Analytics.")

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