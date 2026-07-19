import logging

logger = logging.getLogger(__name__)

def validate_bronze_data(df):
    """Simple schema and null-check gate."""
    required_cols = {'symbol', 'open', 'close'}
    if not required_cols.issubset(df.columns):
        logger.error(f"Validation Failed: Missing columns. Found: {df.columns}")
        return False
    
    null_counts = df['symbol'].isnull().sum()
    if null_counts > 0:
        logger.warning(f"Data Quality Warning: {null_counts} rows have null tickers.")
        return False
        
    return True