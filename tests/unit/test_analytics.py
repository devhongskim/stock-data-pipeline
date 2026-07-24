import os
import duckdb
import pandas as pd
import pytest
from unittest.mock import patch

import analytics


def make_silver_df(rows):
    return pd.DataFrame(rows)


SILVER_ROWS = [
    {"trading_date": "2026-07-17", "ticker": "AAPL", "open_price": 100.0, "high_price": 110.0,
     "low_price": 95.0, "close_price": 105.0, "volume": 1000, "after_hours": 105.5, "pre_market": 99.0},
    {"trading_date": "2026-07-17", "ticker": "MSFT", "open_price": 200.0, "high_price": 220.0,
     "low_price": 190.0, "close_price": 210.0, "volume": 2000, "after_hours": 211.0, "pre_market": 198.0},
]


@pytest.fixture(autouse=True)
def isolate_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


@pytest.fixture(autouse=True)
def patch_client(fake_s3, monkeypatch):
    monkeypatch.setattr(analytics, "get_verified_s3_client", lambda: fake_s3)
    monkeypatch.setattr(analytics, "BUCKET_NAME", "fake-bucket")
    return fake_s3


def seed_silver(fake_s3, tmp_path, yesterday, rows=None):
    if rows is None:
        rows = [dict(r, trading_date=yesterday) for r in SILVER_ROWS]
    silver_key = f"silver/stocks/date={yesterday}/stocks_clean_{yesterday}.parquet"
    local = tmp_path / "seed_silver.parquet"
    make_silver_df(rows).to_parquet(local)
    with open(local, "rb") as f:
        fake_s3.store[silver_key] = f.read()
    return silver_key


class TestFirstRun:
    def test_creates_new_duckdb_and_uploads_gold(self, patch_client, tmp_path):
        yesterday = "2026-07-17"
        silver_key = seed_silver(patch_client, tmp_path, yesterday)

        analytics.generate_gold_metrics(silver_key, yesterday)

        gold_key = f"gold/stocks/date={yesterday}/stocks_metrics_{yesterday}.parquet"
        assert gold_key in patch_client.store
        assert "data/stock_metrics.duckdb" in patch_client.store

    def test_local_temp_files_cleaned_up(self, patch_client, tmp_path):
        yesterday = "2026-07-17"
        silver_key = seed_silver(patch_client, tmp_path, yesterday)

        analytics.generate_gold_metrics(silver_key, yesterday)

        assert not os.path.exists(f"temp_silver_{yesterday}.parquet")
        assert not os.path.exists(f"temp_gold_{yesterday}.parquet")
        assert not os.path.exists("stock_metrics.duckdb")

    def test_daily_return_and_volatility_are_computed_correctly(self, patch_client, tmp_path):
        yesterday = "2026-07-17"
        silver_key = seed_silver(patch_client, tmp_path, yesterday)

        analytics.generate_gold_metrics(silver_key, yesterday)

        with open("check.duckdb", "wb") as f:
            f.write(patch_client.store["data/stock_metrics.duckdb"])
        con = duckdb.connect("check.duckdb")
        row = con.execute(
            "SELECT daily_return, intraday_volatility FROM market_metrics WHERE ticker = 'AAPL'"
        ).fetchone()
        con.close()

        # AAPL: open=100, high=110, low=95, close=105
        # daily_return   = (105-100)/100 * 100 = 5.0
        # volatility     = (110-95)/100 * 100  = 15.0
        assert row == (5.0, 15.0)


class TestUpsertBehavior:
    def test_rerun_same_date_updates_rather_than_duplicates(self, patch_client, tmp_path):
        yesterday = "2026-07-17"

        silver_key = seed_silver(patch_client, tmp_path, yesterday)
        analytics.generate_gold_metrics(silver_key, yesterday)

        # Re-run with a revised close price for AAPL on the same date
        updated_rows = [
            dict(SILVER_ROWS[0], trading_date=yesterday, close_price=120.0),
            dict(SILVER_ROWS[1], trading_date=yesterday),
        ]
        silver_key = seed_silver(patch_client, tmp_path, yesterday, rows=updated_rows)
        analytics.generate_gold_metrics(silver_key, yesterday)

        with open("check2.duckdb", "wb") as f:
            f.write(patch_client.store["data/stock_metrics.duckdb"])
        con = duckdb.connect("check2.duckdb")
        row_count = con.execute(
            "SELECT COUNT(*) FROM market_metrics WHERE ticker = 'AAPL' AND trading_date = '2026-07-17'"
        ).fetchone()[0]
        daily_return = con.execute(
            "SELECT daily_return FROM market_metrics WHERE ticker = 'AAPL' AND trading_date = '2026-07-17'"
        ).fetchone()[0]
        con.close()

        assert row_count == 1  # updated in place, not duplicated
        assert daily_return == 20.0  # (120-100)/100 * 100

    def test_different_date_accumulates_history(self, patch_client, tmp_path):
        silver_key = seed_silver(patch_client, tmp_path, "2026-07-17")
        analytics.generate_gold_metrics(silver_key, "2026-07-17")

        silver_key = seed_silver(patch_client, tmp_path, "2026-07-20")
        analytics.generate_gold_metrics(silver_key, "2026-07-20")

        with open("check3.duckdb", "wb") as f:
            f.write(patch_client.store["data/stock_metrics.duckdb"])
        con = duckdb.connect("check3.duckdb")
        total_rows = con.execute("SELECT COUNT(*) FROM market_metrics").fetchone()[0]
        distinct_dates = con.execute("SELECT COUNT(DISTINCT trading_date) FROM market_metrics").fetchone()[0]
        con.close()

        assert total_rows == 4  # 2 tickers x 2 dates
        assert distinct_dates == 2

    def test_gold_parquet_matches_duckdb_after_run(self, patch_client, tmp_path):
        """Confirms the single-computation refactor: DuckDB and the S3 parquet
        must agree, since both are now derived from the same df_gold object."""
        yesterday = "2026-07-17"
        silver_key = seed_silver(patch_client, tmp_path, yesterday)
        analytics.generate_gold_metrics(silver_key, yesterday)

        gold_key = f"gold/stocks/date={yesterday}/stocks_metrics_{yesterday}.parquet"
        with open("gold_check.parquet", "wb") as f:
            f.write(patch_client.store[gold_key])
        gold_df = pd.read_parquet("gold_check.parquet")

        with open("check4.duckdb", "wb") as f:
            f.write(patch_client.store["data/stock_metrics.duckdb"])
        con = duckdb.connect("check4.duckdb")
        db_rows = con.execute(
            "SELECT ticker, daily_return, intraday_volatility FROM market_metrics ORDER BY ticker"
        ).fetchall()
        con.close()

        parquet_rows = list(
            gold_df.sort_values("ticker")[["ticker", "daily_return", "intraday_volatility"]]
            .itertuples(index=False, name=None)
        )
        assert db_rows == parquet_rows


class TestFailureHandling:
    def test_raises_if_s3_upload_fails(self, patch_client, tmp_path):
        yesterday = "2026-07-17"
        silver_key = seed_silver(patch_client, tmp_path, yesterday)

        def broken_upload(*args, **kwargs):
            raise Exception("S3 is down")

        patch_client.upload_file = broken_upload

        with pytest.raises(RuntimeError, match="Analytic metrics generation failed"):
            analytics.generate_gold_metrics(silver_key, yesterday)

    def test_missing_silver_key_raises(self, patch_client, tmp_path):
        with pytest.raises(RuntimeError, match="Analytic metrics generation failed"):
            analytics.generate_gold_metrics("silver/does/not/exist.parquet", "2026-07-17")