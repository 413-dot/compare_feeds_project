import json

import pytest

from compare_files import config as config_module
from compare_files.config import ConfigError, get_config


def test_get_config_reads_columns_metadata_and_report_layout(tmp_path, monkeypatch):
    config_data = {
        "columns": [
            {
                "name": "recordNumber",
                "columnName": "id",
                "comparison": {
                    "iscompositekey": True,
                    "excludeFromComparison": False,
                    "isReportable": "true",
                },
            },
            {
                "name": "accountSourceCode",
                "comparison": {
                    "iscompositekey": False,
                    "excludeFromComparison": True,
                    "isReportable": "true",
                },
            },
            {
                "name": "accountingControlNumber",
                "comparison": {
                    "iscompositekey": False,
                    "excludeFromComparison": False,
                    "isReportable": True,
                },
            },
        ],
        "report": {
            "columns": [
                {"name": "RecordKey", "source": "RecordKey"},
                {"name": "Err", "source": "ErrorType"},
            ]
        },
    }
    config_path = tmp_path / "comparison.json"
    config_path.write_text(json.dumps(config_data), encoding="utf-8")

    monkeypatch.setenv("COMPARISON_CONFIG_PATH", str(config_path))
    config_module._load_config_file.cache_clear()

    cfg = get_config("teamA")
    assert cfg["compositekey"] == ["id"]
    assert cfg["fieldstocompare"] == ["accountSourceCode"]
    assert cfg["reportfields"] == ["id", "accountSourceCode", "accountingControlNumber"]
    assert cfg["report_columns"] == [
        {"name": "RecordKey", "source": "RecordKey"},
        {"name": "Err", "source": "ErrorType"},
    ]


def test_get_config_requires_composite_key(tmp_path, monkeypatch):
    config_data = {
        "columns": [
            {"name": "a", "comparison": {"iscompositekey": False}}
        ]
    }
    config_path = tmp_path / "comparison.json"
    config_path.write_text(json.dumps(config_data), encoding="utf-8")

    monkeypatch.setenv("COMPARISON_CONFIG_PATH", str(config_path))
    config_module._load_config_file.cache_clear()

    with pytest.raises(ConfigError):
        get_config("teamA")


def test_get_config_uses_default_report_columns_when_missing(tmp_path, monkeypatch):
    config_data = {
        "columns": [
            {"name": "id", "comparison": {"iscompositekey": True}}
        ]
    }
    config_path = tmp_path / "comparison.json"
    config_path.write_text(json.dumps(config_data), encoding="utf-8")

    monkeypatch.setenv("COMPARISON_CONFIG_PATH", str(config_path))
    config_module._load_config_file.cache_clear()

    cfg = get_config("teamA")
    assert [c["name"] for c in cfg["report_columns"]] == [
        "RecordKey",
        "ExistingFileRecordNum",
        "NewFileRecordNum",
        "ColumnName",
        "ExistingValue",
        "NewValue",
        "ErrorType",
    ]
