import tempfile
from pathlib import Path

import boto3


def make_s3_client(settings):
    kwargs: dict = {"region_name": settings.aws_region}
    if settings.aws_endpoint_url:
        kwargs["endpoint_url"] = settings.aws_endpoint_url
    if settings.aws_access_key_id:
        kwargs["aws_access_key_id"] = settings.aws_access_key_id
        kwargs["aws_secret_access_key"] = settings.aws_secret_access_key
    return boto3.client("s3", **kwargs)


def download_to_tempfile(s3_client, bucket: str, key: str) -> Path:
    suffix = Path(key).suffix or ".tmp"
    tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    try:
        s3_client.download_fileobj(bucket, key, tmp)
    finally:
        tmp.close()
    return Path(tmp.name)
