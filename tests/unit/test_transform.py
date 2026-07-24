import json
import os
import duckdb
import pytest
from unittest.mock import patch

import transform


BRONZE_ROWS_OK = [
    {"from": "2026-07-17", "symbol": "AAPL", "status": "OK", "open": 100.0, "high": 105.0,
     "low": 99.0, "close": 103.0, "volume": 1000, "afterHours": 103.5, "preMarket": 99.5},
    {"from": "2026-07-17", "symbol": "MSFT", "status": "OK", "open": 200.0, "high": 210.0,
     "low": 198.0, "close": 205.0, "volume": 2000, "afterHours": 206.0, "preMarket": 199.0},
]


def write_bronze_json(tmp_path, rows):
    path = tmp_path / "source_bronze.json"
    with open(path, "w") as f:
        json.dump(rows, f)
    return path


@pytest.fixture(autouse=True)
def isolate_cwd(tmp_path, monkeypatch):
    """Run each test in its own temp directory so local temp files never collide."""
    monkeypatch.chdir(tmp_path)


@pytest.fixture(autouse=True)
def patch_client(fake_s3, monkeypatch):
    monkeypatch.setattr(transform, "get_verified_s3_client", lambda: fake_s3)
    monkeypatch.setattr(transform, "BUCKET_NAME", "fake-bucket")
    return fake_s3


def seed_bronze(fake_s3, tmp_path, yesterday, rows=None):
    """
    Seed a bronze JSON payload into the fake S3 store for `yesterday`.
    IMPORTANT: transform.py derives `trading_date` from each row's "from" field,
    not from the `yesterday` string used for file naming/keying -- so every row
    must carry the matching "from" date, or upserts will silently target the
    wrong date.
    """
    if rows is None:
        rows = [dict(r, **{"from": yesterday}) for r in BRONZE_ROWS_OK]
    bronze_key = f"bronze/stocks/date={yesterday}/stocks_{yesterday}.json"
    local = write_bronze_json(tmp_path, rows)
    with open(local, "rb") as f:
        fake_s3.store[bronze_key] = f.read()
    return bronze_key


class TestFirstRun:
    def test_creates_new_duckdb_and_returns_silver_key(self, patch_client, tmp_path):
        yesterday = "2026-07-17"
        bronze_key = seed_bronze(patch_client, tmp_path, yesterday)

        with patch("transform.validate_bronze_data", return_value=True):
            result = transform.transform_bronze_to_silver(bronze_key, yesterday)

        assert result == f"silver/stocks/date={yesterday}/stocks_clean_{yesterday}.parquet"
        assert result in patch_client.store
        assert "data/stock_raw.duckdb" in patch_client.store

    def test_local_temp_files_cleaned_up(self, patch_client, tmp_path):
        yesterday = "2026-07-17"
        bronze_key = seed_bronze(patch_client, tmp_path, yesterday)

        with patch("transform.validate_bronze_data", return_value=True):
            transform.transform_bronze_to_silver(bronze_key, yesterday)

        assert not os.path.exists(f"temp_bronze_{yesterday}.json")
        assert not os.path.exists(f"temp_silver_{yesterday}.parquet")
        assert not os.path.exists("stock_raw.duckdb")

    def test_duckdb_table_contains_both_tickers(self, patch_client, tmp_path):
        yesterday = "2026-07-17"
        bronze_key = seed_bronze(patch_client, tmp_path, yesterday)

        with patch("transform.validate_bronze_data", return_value=True):
            transform.transform_bronze_to_silver(bronze_key, yesterday)

        # Pull the uploaded DuckDB warehouse back down and inspect it directly
        warehouse_bytes = patch_client.store["data/stock_raw.duckdb"]
        with open("check.duckdb", "wb") as f:
            f.write(warehouse_bytes)

        con = duckdb.connect("check.duckdb")
        rows = con.execute("SELECT ticker, close_price FROM stock_prices ORDER BY ticker").fetchall()
        con.close()

        assert rows == [("AAPL", 103.0), ("MSFT", 205.0)]


class TestUpsertBehavior:
    def test_rerun_same_date_updates_rather_than_duplicates(self, patch_client, tmp_path):
        yesterday = "2026-07-17"

        # First run
        bronze_key = seed_bronze(patch_client, tmp_path, yesterday)
        with patch("transform.validate_bronze_data", return_value=True):
            transform.transform_bronze_to_silver(bronze_key, yesterday)

        # Second run: same date, AAPL's close price has changed
        updated_rows = [
            dict(BRONZE_ROWS_OK[0], **{"from": yesterday, "close": 999.0}),
            dict(BRONZE_ROWS_OK[1], **{"from": yesterday}),
        ]
        bronze_key = seed_bronze(patch_client, tmp_path, yesterday, rows=updated_rows)
        with patch("transform.validate_bronze_data", return_value=True):
            transform.transform_bronze_to_silver(bronze_key, yesterday)

        warehouse_bytes = patch_client.store["data/stock_raw.duckdb"]
        with open("check2.duckdb", "wb") as f:
            f.write(warehouse_bytes)

        con = duckdb.connect("check2.duckdb")
        row_count = con.execute(
            "SELECT COUNT(*) FROM stock_prices WHERE ticker = 'AAPL' AND trading_date = '2026-07-17'"
        ).fetchone()[0]
        close_price = con.execute(
            "SELECT close_price FROM stock_prices WHERE ticker = 'AAPL' AND trading_date = '2026-07-17'"
        ).fetchone()[0]
        con.close()

        # Upsert should update in place, not add a second row
        assert row_count == 1
        assert close_price == 999.0

    def test_different_date_adds_new_rows_without_touching_old(self, patch_client, tmp_path):
        # Day 1
        bronze_key = seed_bronze(patch_client, tmp_path, "2026-07-17")
        with patch("transform.validate_bronze_data", return_value=True):
            transform.transform_bronze_to_silver(bronze_key, "2026-07-17")

        # Day 2
        bronze_key = seed_bronze(patch_client, tmp_path, "2026-07-20")
        with patch("transform.validate_bronze_data", return_value=True):
            transform.transform_bronze_to_silver(bronze_key, "2026-07-20")

        warehouse_bytes = patch_client.store["data/stock_raw.duckdb"]
        with open("check3.duckdb", "wb") as f:
            f.write(warehouse_bytes)

        con = duckdb.connect("check3.duckdb")
        total_rows = con.execute("SELECT COUNT(*) FROM stock_prices").fetchone()[0]
        distinct_dates = con.execute("SELECT COUNT(DISTINCT trading_date) FROM stock_prices").fetchone()[0]
        con.close()

        assert total_rows == 4  # 2 tickers x 2 dates
        assert distinct_dates == 2


class TestValidationGate:
    def test_invalid_bronze_data_aborts_before_any_upload(self, patch_client, tmp_path):
        yesterday = "2026-07-17"
        bronze_key = seed_bronze(patch_client, tmp_path, yesterday)

        with patch("transform.validate_bronze_data", return_value=False):
            with pytest.raises(RuntimeError, match="Transformation/Load failed"):
                transform.transform_bronze_to_silver(bronze_key, yesterday)

        # Nothing should have been written to S3 beyond the bronze file we seeded ourselves
        assert f"silver/stocks/date={yesterday}/stocks_clean_{yesterday}.parquet" not in patch_client.store
        assert "data/stock_raw.duckdb" not in patch_client.store


class TestFailureHandling:
    def test_raises_if_s3_upload_fails(self, patch_client, tmp_path):
        yesterday = "2026-07-17"
        bronze_key = seed_bronze(patch_client, tmp_path, yesterday)

        def broken_upload(*args, **kwargs):
            raise Exception("S3 is down")

        patch_client.upload_file = broken_upload

        with patch("transform.validate_bronze_data", return_value=True):
            with pytest.raises(RuntimeError, match="Transformation/Load failed"):
                transform.transform_bronze_to_silver(bronze_key, yesterday)

    def test_missing_bronze_key_raises(self, patch_client, tmp_path):
        with pytest.raises(RuntimeError, match="Transformation/Load failed"):
            transform.transform_bronze_to_silver("bronze/does/not/exist.json", "2026-07-17")