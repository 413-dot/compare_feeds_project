import logging
from typing import Dict, List

import pandas as pd

LOG = logging.getLogger(__name__)


def select_report_fields(df: pd.DataFrame, report_columns: List[Dict[str, str]]) -> pd.DataFrame:
    if not report_columns:
        LOG.info("report: no report column mapping configured, returning original frame")
        return df

    out = pd.DataFrame(index=df.index if not df.empty else None)
    selected = []
    for column in report_columns:
        output_name = column.get("name")
        source_name = column.get("source")
        if not output_name:
            continue
        selected.append(output_name)
        if source_name in df.columns:
            out[output_name] = df[source_name]
        else:
            out[output_name] = ""

    LOG.info("report: selected report columns=%s rows=%d", selected, len(out.index))
    return out
