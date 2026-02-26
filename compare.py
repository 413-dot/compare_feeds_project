from typing import Dict, List

import pandas as pd


def _suffix_non_key(df: pd.DataFrame, keys: List[str], suffix: str) -> pd.DataFrame:
    rename = {col: f"{col}__{suffix}" for col in df.columns if col not in keys}
    return df.rename(columns=rename)


def _record_key_for_row(row: pd.Series, composite_key_fields: List[str]) -> str:
    # RecordKey is the configured composite key in order: key1=value1|key2=value2
    pairs = [f"{field}={str(row.get(field, ''))}" for field in composite_key_fields]
    return "|".join(pairs)


def compare_frames(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    ignore_fields: List[str],
    composite_key_fields: List[str],
) -> pd.DataFrame:
    ignore_set = set(ignore_fields or [])
    key_set = set(composite_key_fields or [])

    old_df = old_df.copy().fillna("").astype(str)
    new_df = new_df.copy().fillna("").astype(str)

    old_df["_existing_row_num"] = (old_df.index + 1).astype(str)
    new_df["_new_row_num"] = (new_df.index + 1).astype(str)

    all_fields = sorted((set(old_df.columns) | set(new_df.columns)) - key_set - {"_existing_row_num", "_new_row_num"})
    compare_fields = [field for field in all_fields if field not in ignore_set]

    old_s = _suffix_non_key(old_df, composite_key_fields, "old")
    new_s = _suffix_non_key(new_df, composite_key_fields, "new")
    merged = old_s.merge(new_s, on=composite_key_fields, how="outer", indicator=True)

    report_rows: List[Dict[str, str]] = []

    for _, row in merged.iterrows():
        record_key = _record_key_for_row(row, composite_key_fields)
        existing_record_num = str(row.get("_existing_row_num__old", ""))
        new_record_num = str(row.get("_new_row_num__new", ""))
        merge_state = str(row.get("_merge", ""))

        base_row = {
            "RecordKey": record_key,
            "ExistingFileRecordNum": existing_record_num,
            "NewFileRecordNum": new_record_num,
        }
        for key in composite_key_fields:
            base_row[key] = str(row.get(key, ""))

        if merge_state == "both":
            for field in compare_fields:
                old_val = str(row.get(f"{field}__old", ""))
                new_val = str(row.get(f"{field}__new", ""))
                if old_val != new_val:
                    row_data = dict(base_row)
                    row_data.update(
                        {
                            "ColumnName": field,
                            "ExistingValue": old_val,
                            "NewValue": new_val,
                            "ErrorType": "ColumnValDiff",
                        }
                    )
                    report_rows.append(row_data)
            continue

        if merge_state == "left_only":
            row_data = dict(base_row)
            row_data.update(
                {
                    "ColumnName": "",
                    "ExistingValue": "",
                    "NewValue": "",
                    "ErrorType": "MissingInNewFile",
                }
            )
            report_rows.append(row_data)
            continue

        if merge_state == "right_only":
            row_data = dict(base_row)
            row_data.update(
                {
                    "ColumnName": "",
                    "ExistingValue": "",
                    "NewValue": "",
                    "ErrorType": "MissingInExistingFile",
                }
            )
            report_rows.append(row_data)

    return pd.DataFrame(report_rows)
