import pytest
from moto import mock_aws
import boto3
from pipeline.worker.s3 import download_to_tempfile, make_s3_client
from pipeline.config import Settings


@mock_aws
def test_download_to_tempfile_roundtrip():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")
    content = b"col1,col2\n1,2\n"
    s3.put_object(Bucket="test-bucket", Key="uploads/data.csv", Body=content)

    path = download_to_tempfile(s3, "test-bucket", "uploads/data.csv")
    try:
        assert path.read_bytes() == content
    finally:
        path.unlink(missing_ok=True)


@mock_aws
def test_download_suffix_preserved():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(Bucket="test-bucket", Key="uploads/file.xlsx", Body=b"data")

    path = download_to_tempfile(s3, "test-bucket", "uploads/file.xlsx")
    try:
        assert path.suffix == ".xlsx"
    finally:
        path.unlink(missing_ok=True)


def test_make_s3_client_localstack():
    settings = Settings(
        aws_endpoint_url="http://localhost:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    client = make_s3_client(settings)
    assert client.meta.endpoint_url == "http://localhost:4566"
