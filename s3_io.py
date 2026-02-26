from typing import Dict, List

try:
    import boto3
except ImportError:  # pragma: no cover
    boto3 = None


def _s3_client():
    if boto3 is None:
        raise RuntimeError("boto3 is required for S3 operations")
    return boto3.client("s3")


def list_data_files(bucket: str, prefix: str) -> List[Dict]:
    s3 = _s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    objects: List[Dict] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key", "")
            if key.endswith("/"):
                continue
            if key.endswith("report.csv"):
                continue
            objects.append(obj)
    return objects


def read_object_bytes(bucket: str, key: str) -> bytes:
    s3 = _s3_client()
    resp = s3.get_object(Bucket=bucket, Key=key)
    body = resp["Body"].read()
    return body or b""


def upload_report(bucket: str, key: str, content: str) -> None:
    s3 = _s3_client()
    s3.put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))
