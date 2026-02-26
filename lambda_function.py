import json
import logging
from urllib.parse import unquote_plus

from compare_files.compare import compare_frames
from compare_files.config import ConfigError, get_config
from compare_files.normalize import normalize_df
from compare_files.report import select_report_fields
from compare_files.s3_io import list_data_files, read_object_bytes, upload_report

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)


def _is_csv_file(key: str) -> bool:
    return key.lower().endswith(".csv")


def lambda_handler(event, context):
    LOG.info("Event received: %s", json.dumps(event))

    try:
        bucket = event["detail"]["bucket"]["name"]
        key = unquote_plus(event["detail"]["object"]["key"])
        if key.endswith("report.csv"):
            LOG.info("Skipping report file: %s", key)
            return {"statusCode": 200, "body": "ok"}

        # Expect files under feedId/batch_prefix/filename
        parts = key.split("/")
        if len(parts) < 2:
            LOG.warning("Unexpected key format: %s", key)
            return {"statusCode": 200, "body": "ok"}

        feed_id = parts[0]
        batch_prefix = "/".join(parts[:-1]) + "/"

        config = get_config(feed_id)

        objects = list_data_files(bucket, batch_prefix)
        if len(objects) != 2:
            LOG.warning("Expected 2 files under %s, found %d", batch_prefix, len(objects))
            return {"statusCode": 200, "body": "ok"}

        # Determine old/new by filename: new contains "SB", otherwise old
        sb_objects = [obj for obj in objects if "SB" in obj["Key"]]
        if len(sb_objects) != 1:
            LOG.warning("Expected exactly one file containing 'SB' under %s, found %d", batch_prefix, len(sb_objects))
            return {"statusCode": 200, "body": "ok"}
        new_obj = sb_objects[0]
        old_candidates = [obj for obj in objects if obj["Key"] != new_obj["Key"]]
        if len(old_candidates) != 1:
            LOG.warning("Expected exactly one non-SB file under %s, found %d", batch_prefix, len(old_candidates))
            return {"statusCode": 200, "body": "ok"}
        old_obj = old_candidates[0]

        old_body = read_object_bytes(bucket, old_obj["Key"])
        new_body = read_object_bytes(bucket, new_obj["Key"])

        if not old_body or not new_body:
            LOG.warning("One or both files are empty: %s, %s", old_obj["Key"], new_obj["Key"])
            return {"statusCode": 200, "body": "ok"}

        old_df = normalize_df(old_body, _is_csv_file(old_obj["Key"]))
        new_df = normalize_df(new_body, _is_csv_file(new_obj["Key"]))

        if old_df.empty or new_df.empty:
            LOG.warning("One or both files parsed to 0 rows: %s, %s", old_obj["Key"], new_obj["Key"])
            return {"statusCode": 200, "body": "ok"}

        composite_key = config.get("compositekey", [])
        if not composite_key:
            LOG.warning("Missing compositekey in config for feedId=%s", feed_id)
            return {"statusCode": 200, "body": "ok"}

        missing_keys = [k for k in composite_key if k not in old_df.columns or k not in new_df.columns]
        if missing_keys:
            LOG.warning("Composite keys missing in data: %s", ",".join(missing_keys))
            return {"statusCode": 200, "body": "ok"}

        ignore_fields = config.get("fieldstocompare", [])
        report_columns = config.get("report_columns", [])

        report_df = compare_frames(old_df, new_df, ignore_fields, composite_key)
        report_df = select_report_fields(report_df, report_columns)

        report_csv = report_df.to_csv(index=False)

        report_key = f"reports/{batch_prefix}report.csv"
        upload_report(bucket, report_key, report_csv)
        LOG.info("Report written to s3://%s/%s", bucket, report_key)

    except KeyError:
        LOG.warning("EventBridge event does not contain detail.bucket.name/detail.object.key")
    except ConfigError as exc:
        LOG.error("Config error: %s", exc)
    except Exception as exc:
        LOG.exception("Unhandled error: %s", exc)

    return {"statusCode": 200, "body": "ok"}
