import json
import logging
import os
import time
from urllib.parse import unquote_plus

from compare_files.compare import compare_frames
from compare_files.config import ConfigError, get_config
from compare_files.normalize import normalize_df
from compare_files.report import select_report_fields
from compare_files.s3_io import list_data_files, read_object_bytes, upload_report

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)


def lambda_handler(event, context):
    start_ts = time.time()
    request_id = getattr(context, "aws_request_id", "unknown")
    function_name = getattr(context, "function_name", os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "unknown"))
    configured_path = os.environ.get("COMPARISON_CONFIG_PATH", "/var/task/config/comparison_config.json")

    LOG.info(
        "Invocation started requestId=%s function=%s configuredConfigPath=%s configExists=%s",
        request_id,
        function_name,
        configured_path,
        os.path.exists(configured_path),
    )
    LOG.info("Event received: %s", json.dumps(event))

    try:
        bucket = event["detail"]["bucket"]["name"]
        key = unquote_plus(event["detail"]["object"]["key"])
        LOG.info("Parsed eventBridge object bucket=%s key=%s", bucket, key)
        if key.endswith("report.csv"):
            LOG.info("Skipping report file: %s", key)
            return {"statusCode": 200, "body": "ok"}

        file_name = key
        feed_id = ""
        LOG.info("Using feed context feedId=%s fileName=%s", feed_id, file_name)

        config = get_config(feed_id)
        LOG.info(
            "Loaded config feedId=%s compositeKeys=%d ignoreFields=%d reportColumns=%d",
            feed_id,
            len(config.get("compositekey", [])),
            len(config.get("fieldstocompare", [])),
            len(config.get("report_columns", [])),
        )

        objects = list_data_files(bucket, "")
        LOG.info("Discovered objects under prefix count=%d keys=%s", len(objects), [obj.get("Key", "") for obj in objects])
        if len(objects) != 2:
            LOG.warning("Expected 2 data files in bucket root, found %d", len(objects))
            return {"statusCode": 200, "body": "ok"}

        # Determine old/new by filename: new contains "SB", otherwise old
        sb_objects = [obj for obj in objects if "SB" in obj["Key"]]
        if len(sb_objects) != 1:
            LOG.warning("Expected exactly one file containing 'SB', found %d", len(sb_objects))
            return {"statusCode": 200, "body": "ok"}
        new_obj = sb_objects[0]
        old_candidates = [obj for obj in objects if obj["Key"] != new_obj["Key"]]
        if len(old_candidates) != 1:
            LOG.warning("Expected exactly one non-SB file, found %d", len(old_candidates))
            return {"statusCode": 200, "body": "ok"}
        old_obj = old_candidates[0]
        LOG.info("Selected files oldFile=%s newFile=%s", old_obj["Key"], new_obj["Key"])

        old_body = read_object_bytes(bucket, old_obj["Key"])
        new_body = read_object_bytes(bucket, new_obj["Key"])
        LOG.info("Downloaded files oldBytes=%d newBytes=%d", len(old_body or b""), len(new_body or b""))

        if not old_body or not new_body:
            LOG.warning("One or both files are empty: %s, %s", old_obj["Key"], new_obj["Key"])
            return {"statusCode": 200, "body": "ok"}

        old_df = normalize_df(old_body)
        new_df = normalize_df(new_body)
        LOG.info(
            "Normalized files oldRows=%d oldCols=%d newRows=%d newCols=%d",
            len(old_df.index),
            len(old_df.columns),
            len(new_df.index),
            len(new_df.columns),
        )

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
        LOG.info("Generated report rows=%d cols=%d", len(report_df.index), len(report_df.columns))

        report_csv = report_df.to_csv(index=False)

        report_key = "reports/report.csv"
        upload_report(bucket, report_key, report_csv)
        LOG.info("Report written to s3://%s/%s reportBytes=%d", bucket, report_key, len(report_csv.encode("utf-8")))

    except KeyError:
        LOG.warning("EventBridge event does not contain detail.bucket.name/detail.object.key")
    except ConfigError as exc:
        LOG.error("Config error: %s", exc)
    except Exception as exc:
        LOG.exception("Unhandled error: %s", exc)
    finally:
        duration_ms = int((time.time() - start_ts) * 1000)
        LOG.info("Invocation completed requestId=%s durationMs=%d", request_id, duration_ms)

    return {"statusCode": 200, "body": "ok"}
