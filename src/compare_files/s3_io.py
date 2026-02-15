from typing import Dict, List

import boto3

S3 = boto3.client("s3")


def list_data_files(bucket: str, prefix: str) -> List[Dict]:
    paginator = S3.get_paginator("list_objects_v2")
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
    resp = S3.get_object(Bucket=bucket, Key=key)
    body = resp["Body"].read()
    return body or b""


def upload_report(bucket: str, key: str, content: str) -> None:
    S3.put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))
