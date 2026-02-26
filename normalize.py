import io
import json

import pandas as pd


def _read_csv_df(body: bytes) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(body), dtype=str, keep_default_na=False, na_filter=False)
    return df


def _body_text(body: bytes) -> str:
    # utf-8-sig strips BOM if present.
    return body.decode("utf-8-sig")


def _read_json_df(body: bytes) -> pd.DataFrame:
    text = _body_text(body)
    stripped = text.lstrip()

    read_attempts = []
    if stripped.startswith("["):
        read_attempts = [False, True]
    else:
        read_attempts = [True, False]

    last_error = None
    for lines_mode in read_attempts:
        try:
            df = pd.read_json(io.StringIO(text), lines=lines_mode, dtype=False)
            return df.fillna("").astype(str)
        except ValueError as exc:
            last_error = exc

    # Final fallback: python json parser for edge cases.
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            df = pd.DataFrame(parsed)
        elif isinstance(parsed, dict):
            df = pd.DataFrame([parsed])
        else:
            raise ValueError("JSON root must be object or array")
        return df.fillna("").astype(str)
    except Exception as exc:
        if last_error is not None:
            raise last_error from exc
        raise


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
