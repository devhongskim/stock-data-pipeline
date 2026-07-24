import os
import json
import time
import logging
import requests
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Setup logging
logger = logging.getLogger(__name__)

load_dotenv()

# Constants
TICKERS = ["AAPL", "MSFT", "NVDA", "AMD", "TSLA", "GOOGL", "AMZN", "META", "AVGO", "INTC"]
S3_REGION = "us-east-1"
BUCKET_NAME = os.getenv("AWS_BUCKET")

#Initializes the S3 client and verifies bucket accessibility 
def get_verified_s3_client():
    client = boto3.client('s3', region_name=S3_REGION)
    
    try:
        client.head_bucket(Bucket=BUCKET_NAME)
        logger.info(f"Successfully verified access to S3 bucket.")
    except ClientError as e:
        raise RuntimeError(f"Pipeline startup aborted: Cannot access S3 bucket. {e}")
        
    return client

def check_exists(s3_client, bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False

def fetch_stock_data(yesterday, force_overwrite=False):
    # Initialize and verify S3 client
    s3_client = get_verified_s3_client()

    #Validate API Key
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        raise RuntimeError("🛑 Pipeline startup aborted: Missing POLYGON_API_KEY environment variable.")

    logger.info(f"Starting Weekday Bronze Ingestion for: {yesterday}")
    
    bronze_path = f"bronze/stocks/date={yesterday}/stocks_{yesterday}.json"

    #1 IDEMPOTENCY GATE: Check if data exists BEFORE calling the API
    if not force_overwrite and check_exists(s3_client, BUCKET_NAME, bronze_path):
        logger.info(f"File {bronze_path} already exists. Skipping extraction.")
        return bronze_path  # Return the bronze file for downstream tasks


    extracted_data = []

    try:
        for ticker in TICKERS:
            retries = 0
            max_retries = 2 #total attempts will be max_retries + 1

            while retries <= max_retries:
                url = f"https://api.polygon.io/v1/open-close/{ticker}/{yesterday}?adjusted=true&apiKey={api_key}"
                response = requests.get(url, timeout=10)
                
                if response.status_code == 200:
                    extracted_data.append(response.json())
                    logger.info(f"Successfully extracted data for {ticker}")
                    break
                    # Move to the next ticker after a successful extraction
                elif response.status_code == 429:
                    retries += 1
                    if retries > max_retries:
                        raise RuntimeError(f"🛑 Max retries reached.")
                    logger.warning(f"Rate limited on {ticker}. Perform retry {retries}/{max_retries} after 60 seconds.")
                    time.sleep(60)
                else:
                    raise RuntimeError(f"🛑 Failed to extract data for {ticker}. Unexpected status code {response.status_code}.")
            

            time.sleep(1)

        # 4. AWS S3 Upload
        try:
            if extracted_data:
                s3_client.put_object(
                    Bucket=BUCKET_NAME,
                    Key=bronze_path,
                    Body=json.dumps(extracted_data)
                )
                logger.info(f"Raw payload secured: s3://{BUCKET_NAME}/{bronze_path}")
            else:
                raise RuntimeError(f"🛑 Extractions are empty")
        except Exception as e:
            raise RuntimeError(f"🛑 AWS S3 Upload failed for {yesterday} into bronze tier: {e}")

        logger.info(f"🥉 Bronze layer extraction complete for {yesterday}")
        return bronze_path

    except Exception as e:
        raise RuntimeError(f"🛑 Extraction failed for {yesterday}: {e}")