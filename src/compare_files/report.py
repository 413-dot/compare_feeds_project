from typing import List

import pandas as pd


def select_report_fields(df: pd.DataFrame, report_fields: List[str]) -> pd.DataFrame:
    if df.empty:
        return df

    if not report_fields:
        return df

    fields = list(report_fields)
    for extra in ["compare_status", "diff_fields"]:
        if extra in df.columns and extra not in fields:
            fields.append(extra)

    for col in fields:
        if col not in df.columns:
            df[col] = ""

    return df[fields]
