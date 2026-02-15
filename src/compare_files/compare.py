from typing import List

import pandas as pd


def _suffix_non_key(df: pd.DataFrame, keys: List[str], suffix: str) -> pd.DataFrame:
    rename = {col: f"{col}__{suffix}" for col in df.columns if col not in keys}
    return df.rename(columns=rename)


def compare_frames(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    ignore_fields: List[str],
    composite_key_fields: List[str],
) -> pd.DataFrame:
    ignore_set = set(ignore_fields or [])
    key_set = set(composite_key_fields or [])

    all_fields = sorted((set(old_df.columns) | set(new_df.columns)) - key_set)
    compare_fields = [f for f in all_fields if f not in ignore_set]

    old_df = old_df.fillna("").astype(str)
    new_df = new_df.fillna("").astype(str)

    old_s = _suffix_non_key(old_df, composite_key_fields, "old")
    new_s = _suffix_non_key(new_df, composite_key_fields, "new")

    merged = old_s.merge(new_s, on=composite_key_fields, how="outer", indicator=True)

    def diff_fields_for_row(row) -> str:
        diffs = []
        for field in compare_fields:
            old_col = f"{field}__old"
            new_col = f"{field}__new"
            old_val = row.get(old_col, "")
            new_val = row.get(new_col, "")
            if str(old_val) != str(new_val):
                diffs.append(field)
        return ",".join(diffs)

    diff_fields = merged.apply(diff_fields_for_row, axis=1)

    status = merged["_merge"].map({"left_only": "OLD_ONLY", "right_only": "NEW_ONLY", "both": "BOTH"})
    status = status.mask((status == "BOTH") & (diff_fields == ""), "MATCH")
    status = status.mask((status == "BOTH") & (diff_fields != ""), "DIFF")

    report = pd.DataFrame()
    for field in all_fields:
        old_col = f"{field}__old"
        new_col = f"{field}__new"
        if new_col in merged.columns and old_col in merged.columns:
            report[field] = merged[new_col].where(merged["_merge"] != "left_only", merged[old_col])
        elif new_col in merged.columns:
            report[field] = merged[new_col]
        elif old_col in merged.columns:
            report[field] = merged[old_col]
        else:
            report[field] = ""

    report["compare_status"] = status
    report["diff_fields"] = diff_fields

    return report
