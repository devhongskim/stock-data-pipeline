import pandas as pd
import pytest
from validate import validate_bronze_data
from extract import TICKERS


def make_df(overrides=None):
    """Build a minimal valid bronze DataFrame with one row per expected ticker."""
    rows = [{"symbol": t, "open": 100.0, "close": 101.0} for t in TICKERS]
    df = pd.DataFrame(rows)
    if overrides:
        for key, value in overrides.items():
            df.loc[0, key] = value
    return df


def test_valid_data_passes():
    df = make_df()
    assert validate_bronze_data(df) is True


def test_missing_required_column_fails():
    df = make_df().drop(columns=["close"])
    assert validate_bronze_data(df) is False


def test_null_ticker_fails():
    df = make_df()
    df.loc[0, "symbol"] = None
    assert validate_bronze_data(df) is False


def test_missing_ticker_fails():
    # Drop one full row so a ticker from TICKERS is absent entirely
    df = make_df().iloc[1:].reset_index(drop=True)
    assert validate_bronze_data(df) is False


def test_extra_unexpected_column_does_not_fail():
    df = make_df()
    df["afterHours"] = 1.0
    assert validate_bronze_data(df) is True


def test_empty_dataframe_fails():
    df = pd.DataFrame(columns=["symbol", "open", "close"])
    # No rows at all means every expected ticker is "missing"
    assert validate_bronze_data(df) is False