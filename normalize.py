import io

import pandas as pd


def _detect_json_lines(body: bytes) -> bool:
    sample = body.lstrip()[:1]
    return sample != b"["


def _read_csv_df(body: bytes) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(body), dtype=str, keep_default_na=False, na_filter=False)
    return df


def _read_json_df(body: bytes) -> pd.DataFrame:
    if _detect_json_lines(body):
        df = pd.read_json(io.BytesIO(body), lines=True, dtype=False)
    else:
        df = pd.read_json(io.BytesIO(body), dtype=False)
    df = df.fillna("")
    return df.astype(str)


def normalize_df(body: bytes, is_csv: bool) -> pd.DataFrame:
    if is_csv:
        df = _read_csv_df(body)
    else:
        df = _read_json_df(body)
        # Convert to CSV in-memory then re-read for consistent typing and header handling
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        df = pd.read_csv(io.StringIO(buf.getvalue()), dtype=str, keep_default_na=False, na_filter=False)

    df = df.fillna("")
    return df
