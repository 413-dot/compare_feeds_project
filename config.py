import json
import logging
import os
from functools import lru_cache
from typing import Any, Dict, List


class ConfigError(RuntimeError):
    pass


LOG = logging.getLogger(__name__)


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return False


def _default_config_candidates() -> List[str]:
    module_dir = os.path.dirname(__file__)
    src_dir = os.path.dirname(module_dir)
    return [
        "/var/task/config/comparison_config.json",
        os.path.join(src_dir, "config", "comparison_config.json"),
    ]


@lru_cache(maxsize=1)
def _load_config_file() -> Dict[str, Any]:
    configured_path = os.environ.get("COMPARISON_CONFIG_PATH")
    candidates = [configured_path] if configured_path else _default_config_candidates()

    for config_path in candidates:
        try:
            LOG.info("config: trying local config path=%s", config_path)
            with open(config_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except FileNotFoundError:
            LOG.info("config: config file not found path=%s", config_path)
            continue
        except json.JSONDecodeError as exc:
            raise ConfigError(f"Invalid JSON config file: {config_path}") from exc
        except OSError as exc:
            raise ConfigError(f"Failed to read config file: {config_path}") from exc

        if not isinstance(data, dict):
            raise ConfigError("Config root must be a JSON object")
        LOG.info("config: loaded local config path=%s", config_path)
        return data

    if configured_path:
        raise ConfigError(f"Config file not found: {configured_path}")
    raise ConfigError(f"Config file not found. Tried: {', '.join(candidates)}")


def _default_report_columns() -> List[Dict[str, str]]:
    # Fixed standard report layout in code.
    return [
        {"name": "RecordKey", "source": "RecordKey"},
        {"name": "ExistingFileRecordNum", "source": "ExistingFileRecordNum"},
        {"name": "NewFileRecordNum", "source": "NewFileRecordNum"},
        {"name": "ColumnName", "source": "ColumnName"},
        {"name": "ExistingValue", "source": "ExistingValue"},
        {"name": "NewValue", "source": "NewValue"},
        {"name": "ErrorType", "source": "ErrorType"},
    ]


def _build_config_from_data(data: Dict[str, Any]) -> Dict[str, Any]:
    fields = data.get("fields")
    if not isinstance(fields, list) or not fields:
        # Backward compatibility with older schema.
        fields = data.get("columns", [])
    if not isinstance(fields, list) or not fields:
        raise ConfigError("Config must include a non-empty 'fields' array")

    composite_keys: List[str] = []
    exclude_fields: List[str] = []
    report_fields: List[str] = []  # Reserved for backward compatibility.
    all_field_names: List[str] = []
    display_names: Dict[str, str] = {}

    for field in fields:
        if not isinstance(field, dict):
            continue
        column_name = field.get("columnName") or field.get("name")
        if not column_name:
            continue
        display_name = str(field.get("name") or column_name)
        display_names[str(column_name)] = display_name
        if column_name not in all_field_names:
            all_field_names.append(column_name)

        if _as_bool(field.get("isCompositeKey")) and column_name not in composite_keys:
            composite_keys.append(column_name)

        if (
            _as_bool(field.get("excludesFromComparison"))
            or _as_bool(field.get("excludesfromComparison"))
            or _as_bool(field.get("excludeFromComparison"))
        ) and column_name not in exclude_fields:
            exclude_fields.append(column_name)

        # Support old schema if present.
        comparison = field.get("comparison", {})
        if not isinstance(comparison, dict):
            comparison = {}
        if _as_bool(comparison.get("isCompositeKey")) and column_name not in composite_keys:
            composite_keys.append(column_name)
        if _as_bool(comparison.get("excludeFromComparison")) and column_name not in exclude_fields:
            exclude_fields.append(column_name)
        if _as_bool(comparison.get("isReportable")) and column_name not in report_fields:
            report_fields.append(column_name)

    if not composite_keys:
        raise ConfigError("At least one field must have isCompositeKey=true")

    if not report_fields:
        report_fields = [name for name in all_field_names if name not in exclude_fields]
    ordered_report_fields = composite_keys + [f for f in report_fields if f not in composite_keys]
    LOG.info(
        "config: parsed fields total=%d compositeKeys=%d excluded=%d",
        len(all_field_names),
        len(composite_keys),
        len(exclude_fields),
    )
    return {
        "compositekey": composite_keys,
        # Preferred key name aligned with source config intent.
        "excluded_fields": exclude_fields,
        # Backward compatibility for older callers.
        "fieldstocompare": exclude_fields,
        "reportfields": ordered_report_fields,
        "report_columns": _default_report_columns(),
        "field_display_names": display_names,
    }


def get_config(feed_id: str) -> Dict[str, Any]:
    del feed_id  # Reserved for future per-feed config support.
    data = _load_config_file()
    return _build_config_from_data(data)


def get_config_from_bytes(config_body: bytes, feed_id: str = "") -> Dict[str, Any]:
    del feed_id  # Reserved for future per-feed config support.
    if not config_body:
        raise ConfigError("Config object is empty")
    try:
        data = json.loads(config_body.decode("utf-8-sig"))
        LOG.info("config: loaded config from S3 bytes=%d", len(config_body))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ConfigError("Invalid JSON in config object") from exc
    if not isinstance(data, dict):
        raise ConfigError("Config root must be a JSON object")
    return _build_config_from_data(data)
