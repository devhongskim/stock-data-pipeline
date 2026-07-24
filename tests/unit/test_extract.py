import json
import pytest
from unittest.mock import MagicMock, patch

import extract


@pytest.fixture(autouse=True)
def patch_s3_client(monkeypatch):
    """Prevent real boto3 clients from ever being constructed during tests."""
    fake_client = MagicMock()
    monkeypatch.setattr(extract, "get_verified_s3_client", lambda: fake_client)
    monkeypatch.setattr(extract, "BUCKET_NAME", "fake-bucket")
    monkeypatch.setenv("POLYGON_API_KEY", "fake-key")
    return fake_client


def make_response(status_code, json_body=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_body or {}
    return resp


class TestIdempotency:
    def test_skips_extraction_if_file_already_exists(self, patch_s3_client):
        with patch.object(extract, "check_exists", return_value=True):
            with patch("extract.requests.get") as mock_get:
                result = extract.fetch_stock_data("2026-07-17", force_overwrite=False)
                mock_get.assert_not_called()
        assert result == "bronze/stocks/date=2026-07-17/stocks_2026-07-17.json"

    def test_force_overwrite_ignores_existing_file(self, patch_s3_client):
        with patch.object(extract, "check_exists", return_value=True):
            with patch("extract.requests.get", return_value=make_response(200, {"status": "OK"})):
                with patch("extract.time.sleep"):
                    result = extract.fetch_stock_data("2026-07-17", force_overwrite=True)
        assert result == "bronze/stocks/date=2026-07-17/stocks_2026-07-17.json"
        patch_s3_client.put_object.assert_called_once()


class TestMissingApiKey:
    def test_raises_if_api_key_missing(self, monkeypatch, patch_s3_client):
        monkeypatch.delenv("POLYGON_API_KEY", raising=False)
        with pytest.raises(RuntimeError, match="Missing POLYGON_API_KEY"):
            extract.fetch_stock_data("2026-07-17")


class TestRetryLogic:
    def test_success_on_first_try(self, patch_s3_client):
        with patch.object(extract, "check_exists", return_value=False):
            with patch("extract.requests.get", return_value=make_response(200, {"status": "OK"})) as mock_get:
                with patch("extract.time.sleep"):
                    extract.fetch_stock_data("2026-07-17")
        # One call per ticker, no retries needed
        assert mock_get.call_count == len(extract.TICKERS)

    def test_retries_on_429_then_succeeds(self, patch_s3_client):
        responses = [make_response(429), make_response(200, {"status": "OK"})]
        with patch.object(extract, "check_exists", return_value=False):
            with patch("extract.requests.get", side_effect=responses * len(extract.TICKERS)):
                with patch("extract.time.sleep") as mock_sleep:
                    result = extract.fetch_stock_data("2026-07-17")
        assert result is not None
        assert mock_sleep.called  # confirms the 60s backoff path was exercised

    def test_aborts_after_max_retries_on_persistent_429(self, patch_s3_client):
        with patch.object(extract, "check_exists", return_value=False):
            with patch("extract.requests.get", return_value=make_response(429)):
                with patch("extract.time.sleep"):
                    with pytest.raises(RuntimeError, match="Extraction failed"):
                        extract.fetch_stock_data("2026-07-17")

    def test_total_attempts_is_max_retries_plus_one(self, patch_s3_client):
        """Confirms the off-by-one fix: max_retries=2 means 3 total attempts per ticker."""
        call_count = {"n": 0}

        def counting_get(*args, **kwargs):
            call_count["n"] += 1
            return make_response(429)

        with patch.object(extract, "check_exists", return_value=False):
            with patch("extract.requests.get", side_effect=counting_get):
                with patch("extract.time.sleep"):
                    with pytest.raises(RuntimeError):
                        extract.fetch_stock_data("2026-07-17")
        # Fails on the first ticker only (all-or-nothing abort), 3 attempts for it
        assert call_count["n"] == 3

    def test_unexpected_status_code_aborts_immediately(self, patch_s3_client):
        with patch.object(extract, "check_exists", return_value=False):
            with patch("extract.requests.get", return_value=make_response(500)) as mock_get:
                with patch("extract.time.sleep"):
                    with pytest.raises(RuntimeError, match="Extraction failed"):
                        extract.fetch_stock_data("2026-07-17")
        # All-or-nothing: aborts on first ticker's bad response, doesn't try the rest
        assert mock_get.call_count == 1


class TestS3Upload:
    def test_raises_if_s3_upload_fails(self, patch_s3_client):
        patch_s3_client.put_object.side_effect = Exception("S3 is down")
        with patch.object(extract, "check_exists", return_value=False):
            with patch("extract.requests.get", return_value=make_response(200, {"status": "OK"})):
                with patch("extract.time.sleep"):
                    with pytest.raises(RuntimeError, match="Extraction failed"):
                        extract.fetch_stock_data("2026-07-17")

    def test_returns_bronze_path_on_success(self, patch_s3_client):
        with patch.object(extract, "check_exists", return_value=False):
            with patch("extract.requests.get", return_value=make_response(200, {"status": "OK"})):
                with patch("extract.time.sleep"):
                    result = extract.fetch_stock_data("2026-07-17")
        assert result == "bronze/stocks/date=2026-07-17/stocks_2026-07-17.json"
        # Confirm the payload was actually JSON-serializable and uploaded
        _, kwargs = patch_s3_client.put_object.call_args
        json.loads(kwargs["Body"])  # should not raise