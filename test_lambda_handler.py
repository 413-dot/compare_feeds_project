from datetime import datetime

import lambda_function as lf


def test_lambda_handler_happy_path(monkeypatch):
    event = {
        "source": "aws.s3",
        "detail-type": "Object Created",
        "detail": {
            "bucket": {"name": "input-bucket"},
            "object": {"key": "old.csv"},
        },
    }

    seen = {}

    def fake_get_config(feed_id):
        seen["feed_id"] = feed_id
        return {
            "fieldstocompare": [],
            "compositekey": ["id"],
            "report_columns": [
                {"name": "RecordKey", "source": "RecordKey"},
                {"name": "ColumnName", "source": "ColumnName"},
                {"name": "ErrorType", "source": "ErrorType"},
            ],
        }

    def fake_list(bucket, prefix):
        assert prefix == ""
        return [
            {"Key": "OLD_FILE.csv", "LastModified": datetime(2026, 2, 11, 10, 0, 0)},
            {"Key": "NEW_SB_FILE.csv", "LastModified": datetime(2026, 2, 11, 11, 0, 0)},
        ]

    def fake_read(bucket, key):
        if "SB" not in key:
            return b"id,name\n1,Alice\n"
        return b"id,name\n1,Alicia\n"

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
    assert uploaded["key"] == "reports/report.csv"
    assert "ColumnName" in uploaded["content"]
    assert "ErrorType" in uploaded["content"]
    assert "ColumnValDiff" in uploaded["content"]
    assert seen["feed_id"] == ""


def test_lambda_handler_skips_on_bad_count(monkeypatch):
    event = {
        "source": "aws.s3",
        "detail-type": "Object Created",
        "detail": {
            "bucket": {"name": "input-bucket"},
            "object": {"key": "file.csv"},
        },
    }

    seen = {}

    def fake_get_config(feed_id):
        seen["feed_id"] = feed_id
        return {
            "fieldstocompare": [],
            "compositekey": ["id"],
            "report_columns": [{"name": "RecordKey", "source": "RecordKey"}],
        }

    def fake_list(bucket, prefix):
        assert prefix == ""
        return [{"Key": "OLD_FILE.csv", "LastModified": datetime(2026, 2, 11, 10, 0, 0)}]

    monkeypatch.setattr(lf, "get_config", fake_get_config)
    monkeypatch.setattr(lf, "list_data_files", fake_list)

    result = lf.lambda_handler(event, None)
    assert result["statusCode"] == 200
    assert seen["feed_id"] == ""
