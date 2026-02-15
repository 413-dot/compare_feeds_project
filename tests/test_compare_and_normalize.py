import json

import pandas as pd
import pytest

from compare_files.normalize import normalize_df
from compare_files.compare import compare_frames
from compare_files.report import select_report_fields


def test_normalize_csv_bytes():
    body = b"id,name\n1,Alice\n2,Bob\n"
    df = normalize_df(body, is_csv=True)
    assert list(df.columns) == ["id", "name"]
    assert df.loc[0, "id"] == "1"
    assert df.loc[1, "name"] == "Bob"


def test_normalize_json_array_bytes():
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    body = json.dumps(data).encode("utf-8")
    df = normalize_df(body, is_csv=False)
    assert list(df.columns) == ["id", "name"]
    assert df.loc[0, "id"] == "1"


def test_normalize_json_lines_bytes():
    lines = "\n".join([
        json.dumps({"id": 1, "name": "Alice"}),
        json.dumps({"id": 2, "name": "Bob"}),
    ])
    df = normalize_df(lines.encode("utf-8"), is_csv=False)
    assert list(df.columns) == ["id", "name"]
    assert df.loc[1, "name"] == "Bob"


def test_compare_frames_diff_and_match():
    old_df = pd.DataFrame([
        {"id": "1", "name": "Alice", "amount": "10"},
        {"id": "2", "name": "Bob", "amount": "20"},
    ])
    new_df = pd.DataFrame([
        {"id": "1", "name": "Alice", "amount": "10"},
        {"id": "2", "name": "Bob", "amount": "30"},
        {"id": "3", "name": "Cara", "amount": "40"},
    ])

    report = compare_frames(old_df, new_df, ignore_fields=[], composite_key_fields=["id"])
    by_id = {row["id"]: row for _, row in report.iterrows()}

    assert by_id["1"]["compare_status"] == "MATCH"
    assert by_id["2"]["compare_status"] == "DIFF"
    assert by_id["2"]["diff_fields"] == "amount"
    assert by_id["3"]["compare_status"] == "NEW_ONLY"


def test_compare_frames_ignores_fields():
    old_df = pd.DataFrame([{"id": "1", "name": "A", "ts": "1"}])
    new_df = pd.DataFrame([{"id": "1", "name": "B", "ts": "2"}])

    report = compare_frames(old_df, new_df, ignore_fields=["ts"], composite_key_fields=["id"])
    row = report.iloc[0]
    assert row["compare_status"] == "DIFF"
    assert row["diff_fields"] == "name"


def test_select_report_fields_adds_status_columns():
    df = pd.DataFrame([
        {"id": "1", "name": "A", "compare_status": "MATCH", "diff_fields": ""}
    ])

    selected = select_report_fields(df, ["id"])
    assert list(selected.columns) == ["id", "compare_status", "diff_fields"]
