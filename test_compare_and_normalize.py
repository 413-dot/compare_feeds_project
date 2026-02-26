import json

import pandas as pd

from compare_files.compare import compare_frames
from compare_files.normalize import normalize_df
from compare_files.report import select_report_fields


def test_normalize_csv_bytes():
    body = b"id,name\n1,Alice\n2,Bob\n"
    df = normalize_df(body, is_csv=True)
    assert list(df.columns) == ["id", "name"]
    assert df.loc[0, "id"] == "1"
    assert df.loc[1, "name"] == "Bob"


def test_normalize_tsv_bytes():
    body = b"id\tname\n1\tAlice\n2\tBob\n"
    df = normalize_df(body, is_csv=True)
    assert list(df.columns) == ["id", "name"]
    assert df.loc[0, "id"] == "1"
    assert df.loc[1, "name"] == "Bob"


def test_normalize_tsv_bytes_auto_detect_without_extension_hint():
    body = b"id\tname\n1\tAlice\n2\tBob\n"
    df = normalize_df(body)
    assert list(df.columns) == ["id", "name"]
    assert df.loc[0, "id"] == "1"
    assert df.loc[1, "name"] == "Bob"


def test_normalize_json_array_bytes():
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    body = json.dumps(data).encode("utf-8")
    df = normalize_df(body, is_csv=False)
    assert list(df.columns) == ["id", "name"]
    assert df.loc[0, "id"] == "1"


def test_normalize_json_array_with_bom_bytes():
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    body = ("\ufeff" + json.dumps(data)).encode("utf-8")
    df = normalize_df(body, is_csv=False)
    assert list(df.columns) == ["id", "name"]
    assert df.loc[1, "name"] == "Bob"


def test_normalize_json_lines_bytes():
    lines = "\n".join([
        json.dumps({"id": 1, "name": "Alice"}),
        json.dumps({"id": 2, "name": "Bob"}),
    ])
    df = normalize_df(lines.encode("utf-8"), is_csv=False)
    assert list(df.columns) == ["id", "name"]
    assert df.loc[1, "name"] == "Bob"


def test_normalize_concatenated_json_objects_bytes():
    body = b'{"id":1,"name":"Alice"}{"id":2,"name":"Bob"}'
    df = normalize_df(body, is_csv=False)
    assert list(df.columns) == ["id", "name"]
    assert len(df.index) == 2
    assert df.loc[1, "name"] == "Bob"


def test_normalize_json_array_auto_detect_without_hint():
    body = b'[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]'
    df = normalize_df(body)
    assert list(df.columns) == ["id", "name"]
    assert len(df.index) == 2


def test_compare_frames_generates_column_level_diffs():
    old_df = pd.DataFrame([
        {"id": "1", "name": "Alice", "amount": "10"},
        {"id": "2", "name": "Bob", "amount": "20"},
    ])
    new_df = pd.DataFrame([
        {"id": "1", "name": "Alice", "amount": "10"},
        {"id": "2", "name": "Bobby", "amount": "30"},
    ])

    report = compare_frames(old_df, new_df, ignore_fields=[], composite_key_fields=["id"])

    assert len(report) == 2
    assert set(report["ColumnName"].tolist()) == {"name", "amount"}
    assert set(report["ErrorType"].tolist()) == {"ColumnValDiff"}
    assert (report["RecordKey"] == "id=2").all()




def test_record_key_uses_composite_key_fields_in_order():
    old_df = pd.DataFrame([{"id": "1", "sub": "A", "name": "old"}])
    new_df = pd.DataFrame([{"id": "1", "sub": "A", "name": "new"}])

    report = compare_frames(old_df, new_df, ignore_fields=[], composite_key_fields=["id", "sub"])

    assert len(report) == 1
    assert report.iloc[0]["RecordKey"] == "id=1|sub=A"
def test_compare_frames_ignores_excluded_fields():
    old_df = pd.DataFrame([{"id": "1", "name": "A", "ts": "1"}])
    new_df = pd.DataFrame([{"id": "1", "name": "A", "ts": "2"}])

    report = compare_frames(old_df, new_df, ignore_fields=["ts"], composite_key_fields=["id"])
    assert report.empty


def test_select_report_fields_uses_configured_mapping():
    df = pd.DataFrame([
        {
            "RecordKey": "rk1",
            "ExistingFileRecordNum": "10",
            "NewFileRecordNum": "11",
            "ColumnName": "status",
            "ExistingValue": "A",
            "NewValue": "B",
            "ErrorType": "ColumnValDiff",
            "InternalOnly": "x",
        }
    ])

    selected = select_report_fields(
        df,
        [
            {"name": "RecordKey", "source": "RecordKey"},
            {"name": "Err", "source": "ErrorType"},
            {"name": "FutureColumn", "source": "MissingSource"},
        ],
    )

    assert list(selected.columns) == ["RecordKey", "Err", "FutureColumn"]
    assert selected.iloc[0]["Err"] == "ColumnValDiff"
    assert selected.iloc[0]["FutureColumn"] == ""
