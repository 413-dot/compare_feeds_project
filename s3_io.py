from typing import Dict, List
import logging

try:
    import boto3
except ImportError:  # pragma: no cover
    boto3 = None

LOG = logging.getLogger(__name__)


def _s3_client():
    if boto3 is None:
        raise RuntimeError("boto3 is required for S3 operations")
    return boto3.client("s3")


def list_data_files(bucket: str, prefix: str) -> List[Dict]:
    s3 = _s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    objects: List[Dict] = []
    LOG.info("s3_io: listing objects bucket=%s prefix=%s", bucket, prefix)
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key", "")
            if key.endswith("/"):
                continue
            if key.endswith("report.csv"):
                continue
            objects.append(obj)
    LOG.info("s3_io: listed objects count=%d", len(objects))
    return objects


def read_object_bytes(bucket: str, key: str) -> bytes:
    s3 = _s3_client()
    LOG.info("s3_io: reading object bucket=%s key=%s", bucket, key)
    resp = s3.get_object(Bucket=bucket, Key=key)
    body = resp["Body"].read()
    LOG.info("s3_io: read bytes=%d key=%s", len(body or b""), key)
    return body or b""


def upload_report(bucket: str, key: str, content: str) -> None:
    s3 = _s3_client()
    LOG.info("s3_io: uploading report bucket=%s key=%s bytes=%d", bucket, key, len(content.encode("utf-8")))
    s3.put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))
