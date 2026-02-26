import json
import os
from functools import lru_cache
from typing import Any, Dict, List


class ConfigError(RuntimeError):
    pass


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return False


@lru_cache(maxsize=1)
def _load_config_file() -> Dict[str, Any]:
    config_path = os.environ.get("COMPARISON_CONFIG_PATH", "/var/task/config/comparison_config.json")
    try:
        with open(config_path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError as exc:
        raise ConfigError(f"Config file not found: {config_path}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(f"Invalid JSON config file: {config_path}") from exc
    except OSError as exc:
        raise ConfigError(f"Failed to read config file: {config_path}") from exc

    if not isinstance(data, dict):
        raise ConfigError("Config root must be a JSON object")
    return data


def _default_report_columns() -> List[Dict[str, str]]:
    # Mirrors the requested sample report format.
    return [
        {"name": "RecordKey", "source": "RecordKey"},
        {"name": "ExistingFileRecordNum", "source": "ExistingFileRecordNum"},
        {"name": "NewFileRecordNum", "source": "NewFileRecordNum"},
        {"name": "ColumnName", "source": "ColumnName"},
        {"name": "ExistingValue", "source": "ExistingValue"},
        {"name": "NewValue", "source": "NewValue"},
        {"name": "ErrorType", "source": "ErrorType"},
    ]


def _parse_report_columns(data: Dict[str, Any]) -> List[Dict[str, str]]:
    report_cfg = data.get("report", {})
    if not isinstance(report_cfg, dict):
        return _default_report_columns()

    columns = report_cfg.get("columns", [])
    if not isinstance(columns, list) or not columns:
        return _default_report_columns()

    parsed: List[Dict[str, str]] = []
    for item in columns:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        source = item.get("source")
        if not name or not source:
            continue
        parsed.append({"name": str(name), "source": str(source)})

    return parsed or _default_report_columns()


def get_config(feed_id: str) -> Dict[str, Any]:
    del feed_id  # Reserved for future per-feed config support.
    data = _load_config_file()
    columns = data.get("columns", [])
    if not isinstance(columns, list) or not columns:
        raise ConfigError("Config must include a non-empty 'columns' array")

    composite_keys: List[str] = []
    exclude_fields: List[str] = []
    report_fields: List[str] = []

    for column in columns:
        if not isinstance(column, dict):
            continue
        column_name = column.get("columnName") or column.get("name")
        if not column_name:
            continue
        comparison = column.get("comparison", {})
        if not isinstance(comparison, dict):
            comparison = {}

        if _as_bool(comparison.get("iscompositekey")) and column_name not in composite_keys:
            composite_keys.append(column_name)

        if _as_bool(comparison.get("excludeFromComparison")) and column_name not in exclude_fields:
            exclude_fields.append(column_name)

        if _as_bool(comparison.get("isReportable")) and column_name not in report_fields:
            report_fields.append(column_name)

    if not composite_keys:
        raise ConfigError("At least one column must have comparison.iscompositekey=true")

    ordered_report_fields = composite_keys + [f for f in report_fields if f not in composite_keys]
    return {
        "compositekey": composite_keys,
        "fieldstocompare": exclude_fields,
        "reportfields": ordered_report_fields,
        "report_columns": _parse_report_columns(data),
    }
