import io
import pytest
from botocore.exceptions import ClientError


class FakeS3Client:
    """
    In-memory stand-in for boto3's S3 client. Backs download_file/upload_file/
    put_object with a plain dict so transform.py/analytics.py can run their real
    logic (including real DuckDB upserts) against fake, fast, local "S3" storage.
    """

    def __init__(self):
        self.store = {}  # key -> bytes

    def head_bucket(self, Bucket):
        return {}

    def head_object(self, Bucket, Key):
        if Key not in self.store:
            raise ClientError(
                error_response={"Error": {"Code": "404", "Message": "Not Found"}},
                operation_name="HeadObject",
            )
        return {}

    def download_file(self, Bucket, Key, Filename):
        if Key not in self.store:
            raise ClientError(
                error_response={"Error": {"Code": "404", "Message": "Not Found"}},
                operation_name="GetObject",
            )
        with open(Filename, "wb") as f:
            f.write(self.store[Key])

    def upload_file(self, Filename, Bucket, Key):
        with open(Filename, "rb") as f:
            self.store[Key] = f.read()

    def put_object(self, Bucket, Key, Body):
        if isinstance(Body, str):
            Body = Body.encode("utf-8")
        self.store[Key] = Body


@pytest.fixture
def fake_s3():
    return FakeS3Client()