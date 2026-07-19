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
TICKERS = ["AAPL", "MSFT", "NVDA", "AMD", "TSLA"]
S3_REGION = "us-east-1"
BUCKET_NAME = os.getenv("AWS_BRONZE_BUCKET")

# Initialize S3 client once at the module level
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=S3_REGION
)

def check_exists(s3_client, bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False

def fetch_stock_data(yesterday, force_overwrite=False):
    logger.info(f"Starting Weekday Bronze Ingestion for: {yesterday}")

    s3_key = f"bronze/stocks/date={yesterday}/stocks_{yesterday}.json"

    #1 IDEMPOTENCY GATE: Check if data exists BEFORE calling the API
    if not force_overwrite and check_exists(s3_client, BUCKET_NAME, s3_key):
        logger.info(f"File {s3_key} already exists. Skipping extraction.")
        return None

    #2 API EXTRACTION
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        logger.error("Missing POLYGON_API_KEY!")
        return None

    extracted_data = []

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
                if retries == max_retries + 1:
                    logger.error(f"Max retries reached. Aborting.")
                    return None
                logger.warning(f"Rate limited on {ticker}. Perform retry {retries}/{max_retries} after 60 seconds.")
                time.sleep(60)
            else:
                logger.warning(f"Failed to extract data for {ticker}. Unexpected status code {response.status_code}. Aborting")
                return None
        

        time.sleep(1)

    # AWS S3 Upload
    if extracted_data:
        local_bronze = f"temp_stocks_{yesterday}.json"
        with open(local_bronze, 'w') as f:
            json.dump(extracted_data, f)

        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=json.dumps(extracted_data)
            )
            logger.info(f"Raw payload secured: s3://{BUCKET_NAME}/{s3_key}")
        except Exception as e:
            logger.error(f"AWS S3 Upload Failed: {e}")
            if os.path.exists(local_bronze): 
                os.remove(local_bronze)
            return None
    return local_bronze