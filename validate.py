import logging
from extract import TICKERS

logger = logging.getLogger(__name__)

def validate_bronze_data(df):
    """Simple schema and null-check gate."""
    # 1. Schema Validation
    required_cols = {'symbol', 'open', 'close'}
    if not required_cols.issubset(df.columns):
        logger.error(f"Validation Failed: Missing columns. Found: {df.columns}")
        return False
    
    # 2. Null Check
    null_counts = df['symbol'].isnull().sum()
    if null_counts > 0:
        logger.warning(f"Data Quality Warning: {null_counts} rows have null tickers.")
        return False
        
    # 3. Completeness Check: Ensure all expected tickers are present
    missing_tickers = set(TICKERS) - set(df['symbol'].unique())
    if missing_tickers:
        logger.warning(f"Data Quality Warning: Missing tickers: {missing_tickers}")
        return False

    return True