from typing import Dict, List

import pandas as pd


def select_report_fields(df: pd.DataFrame, report_columns: List[Dict[str, str]]) -> pd.DataFrame:
    if not report_columns:
        return df

    out = pd.DataFrame(index=df.index if not df.empty else None)
    for column in report_columns:
        output_name = column.get("name")
        source_name = column.get("source")
        if not output_name:
            continue
        if source_name in df.columns:
            out[output_name] = df[source_name]
        else:
            out[output_name] = ""

    return out
