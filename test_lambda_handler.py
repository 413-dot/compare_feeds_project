import json
from datetime import datetime

import pandas as pd

import lambda_function as lf


def test_lambda_handler_happy_path(monkeypatch):
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "input-bucket"},
                    "object": {"key": "teamA/2026-02-11/batch-001/old.csv"},
                }
            }
        ]
    }

    def fake_get_config(feed_id):
        return {
            "isoldfilecsv": True,
            "isnewfilecsv": True,
            "fieldstocompare": [],
            "reportfields": ["id"],
            "compositekey": ["id"],
        }

    def fake_list(bucket, prefix):
        return [
            {"Key": "teamA/2026-02-11/batch-001/OLD_FILE.csv", "LastModified": datetime(2026, 2, 11, 10, 0, 0)},
            {"Key": "teamA/2026-02-11/batch-001/NEW_SB_FILE.csv", "LastModified": datetime(2026, 2, 11, 11, 0, 0)},
        ]

    def fake_read(bucket, key):
        if "SB" not in key:
            return b"id,name\n1,Alice\n"
        return b"id,name\n1,Alice\n"

    uploaded = {}

    def fake_upload(bucket, key, content):
        uploaded["bucket"] = bucket
        uploaded["key"] = key
        uploaded["content"] = content

    monkeypatch.setattr(lf, "get_config", fake_get_config)
    monkeypatch.setattr(lf, "list_data_files", fake_list)
    monkeypatch.setattr(lf, "read_object_bytes", fake_read)
    monkeypatch.setattr(lf, "upload_report", fake_upload)

    result = lf.lambda_handler(event, None)

    assert result["statusCode"] == 200
    assert uploaded["bucket"] == "input-bucket"
    assert uploaded["key"].endswith("reports/teamA/2026-02-11/batch-001/report.csv")
    assert "compare_status" in uploaded["content"]


def test_lambda_handler_skips_on_bad_count(monkeypatch):
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "input-bucket"},
                    "object": {"key": "teamA/2026-02-11/batch-001/file.csv"},
                }
            }
        ]
    }

    def fake_get_config(feed_id):
        return {
            "isoldfilecsv": True,
            "isnewfilecsv": True,
            "fieldstocompare": [],
            "reportfields": ["id"],
            "compositekey": ["id"],
        }

    def fake_list(bucket, prefix):
        return [{"Key": "teamA/2026-02-11/batch-001/OLD_FILE.csv", "LastModified": datetime(2026, 2, 11, 10, 0, 0)}]

    monkeypatch.setattr(lf, "get_config", fake_get_config)
    monkeypatch.setattr(lf, "list_data_files", fake_list)

    result = lf.lambda_handler(event, None)
    assert result["statusCode"] == 200
