import io
import json
import csv
import logging
from typing import Optional

import pandas as pd

LOG = logging.getLogger(__name__)


def _detect_delimiter(text: str) -> str:
    sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t|;")
        return dialect.delimiter
    except csv.Error:
        # Fallback to the most frequent known delimiter in the first line.
        first_line = sample.splitlines()[0] if sample else ""
        counts = {
            ",": first_line.count(","),
            "\t": first_line.count("\t"),
            "|": first_line.count("|"),
            ";": first_line.count(";"),
        }
        delimiter, freq = max(counts.items(), key=lambda item: item[1])
        return delimiter if freq > 0 else ","


def _read_csv_df(body: bytes) -> pd.DataFrame:
    text = _body_text(body)
    delimiter = _detect_delimiter(text)
    LOG.info("normalize: reading delimited file delimiter=%s", repr(delimiter))
    df = pd.read_csv(
        io.StringIO(text),
        sep=delimiter,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
    )
    return df


def _body_text(body: bytes) -> str:
    # utf-8-sig strips BOM if present.
    return body.decode("utf-8-sig")


def _looks_like_json(text: str) -> bool:
    stripped = text.lstrip()
    return stripped.startswith("{") or stripped.startswith("[")


def _parse_multiple_json_values(text: str):
    decoder = json.JSONDecoder()
    idx = 0
    length = len(text)
    values = []

    while idx < length:
        while idx < length and text[idx].isspace():
            idx += 1
        if idx >= length:
            break
        value, next_idx = decoder.raw_decode(text, idx)
        values.append(value)
        idx = next_idx

    return values


def _read_json_df(body: bytes) -> pd.DataFrame:
    text = _body_text(body)
    stripped = text.lstrip()
    LOG.info("normalize: reading JSON payload bytes=%d", len(body))

    read_attempts = []
    if stripped.startswith("["):
        read_attempts = [False, True]
    else:
        read_attempts = [True, False]

    last_error = None
    for lines_mode in read_attempts:
        try:
            LOG.info("normalize: trying pandas json parser lines=%s", lines_mode)
            df = pd.read_json(io.StringIO(text), lines=lines_mode, dtype=False)
            LOG.info("normalize: pandas json parser succeeded lines=%s rows=%d", lines_mode, len(df.index))
            return df.fillna("").astype(str)
        except ValueError as exc:
            last_error = exc
            LOG.warning("normalize: pandas json parser failed lines=%s error=%s", lines_mode, exc)

    # Final fallback: python json parser for edge cases.
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            rows = parsed
        elif isinstance(parsed, dict):
            rows = [parsed]
        else:
            raise ValueError("JSON root must be object or array")
        df = pd.DataFrame(rows)
        LOG.info("normalize: python json parser succeeded rows=%d", len(df.index))
        return df.fillna("").astype(str)
    except Exception as exc:
        LOG.warning("normalize: python json parser failed error=%s", exc)

    # Fallback for concatenated JSON objects/arrays without newlines.
    try:
        values = _parse_multiple_json_values(text)
        rows = []
        for value in values:
            if isinstance(value, list):
                rows.extend(value)
            elif isinstance(value, dict):
                rows.append(value)
        if not rows:
            raise ValueError("No object rows parsed from concatenated JSON values")
        df = pd.DataFrame(rows)
        LOG.info("normalize: multi-json fallback succeeded values=%d rows=%d", len(values), len(df.index))
        return df.fillna("").astype(str)
    except Exception as exc:
        LOG.error("normalize: multi-json fallback failed error=%s", exc)
        if last_error is not None:
            raise last_error from exc
        raise


def normalize_df(body: bytes, is_csv: Optional[bool] = None) -> pd.DataFrame:
    text = _body_text(body)
    preferred_is_json = _looks_like_json(text)

    if is_csv is True:
        LOG.info("normalize: forced delimited parsing by hint")
        return _read_csv_df(body).fillna("").astype(str)

    if is_csv is False:
        LOG.info("normalize: forced JSON parsing by hint")
        try:
            return _read_json_df(body).fillna("").astype(str)
        except Exception as exc:
            LOG.warning("normalize: JSON hint failed, falling back to delimited parser error=%s", exc)
            return _read_csv_df(body).fillna("").astype(str)

    if preferred_is_json:
        LOG.info("normalize: auto-detect chose JSON-first strategy")
        try:
            return _read_json_df(body).fillna("").astype(str)
        except Exception as exc:
            LOG.warning("normalize: JSON-first failed, trying delimited parser error=%s", exc)
            return _read_csv_df(body).fillna("").astype(str)

    LOG.info("normalize: auto-detect chose delimited-first strategy")
    try:
        return _read_csv_df(body).fillna("").astype(str)
    except Exception as exc:
        LOG.warning("normalize: delimited-first failed, trying JSON parser error=%s", exc)
        return _read_json_df(body).fillna("").astype(str)
