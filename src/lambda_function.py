import json
import logging
from urllib.parse import unquote_plus

from compare_files.config import ConfigError, get_config
from compare_files.s3_io import list_data_files, read_object_bytes, upload_report
from compare_files.normalize import normalize_df
from compare_files.compare import compare_frames
from compare_files.report import select_report_fields

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)


def lambda_handler(event, context):
    LOG.info("Event received: %s", json.dumps(event))

    for record in event.get("Records", []):
        try:
            bucket = record["s3"]["bucket"]["name"]
            key = unquote_plus(record["s3"]["object"]["key"])
            if key.endswith("report.csv"):
                LOG.info("Skipping report file: %s", key)
                continue

            # Expect files under feedId/batch_prefix/filename
            parts = key.split("/")
            if len(parts) < 2:
                LOG.warning("Unexpected key format: %s", key)
                continue

            feed_id = parts[0]
            batch_prefix = "/".join(parts[:-1]) + "/"

            config = get_config(feed_id)

            objects = list_data_files(bucket, batch_prefix)
            if len(objects) != 2:
                LOG.warning("Expected 2 files under %s, found %d", batch_prefix, len(objects))
                continue

            # Determine old/new by LastModified (older is old)
            objects_sorted = sorted(objects, key=lambda o: o["LastModified"])
            old_obj, new_obj = objects_sorted[0], objects_sorted[1]

            old_body = read_object_bytes(bucket, old_obj["Key"])
            new_body = read_object_bytes(bucket, new_obj["Key"])

            if not old_body or not new_body:
                LOG.warning("One or both files are empty: %s, %s", old_obj["Key"], new_obj["Key"])
                continue

            old_df = normalize_df(old_body, config.get("isoldfilecsv", True))
            new_df = normalize_df(new_body, config.get("isnewfilecsv", True))

            if old_df.empty or new_df.empty:
                LOG.warning("One or both files parsed to 0 rows: %s, %s", old_obj["Key"], new_obj["Key"])
                continue

            composite_key = config.get("compositekey", [])
            if not composite_key:
                LOG.warning("Missing compositekey in config for feedId=%s", feed_id)
                continue

            missing_keys = [k for k in composite_key if k not in old_df.columns or k not in new_df.columns]
            if missing_keys:
                LOG.warning("Composite keys missing in data: %s", ",".join(missing_keys))
                continue

            ignore_fields = config.get("fieldstocompare", [])

            report_df = compare_frames(old_df, new_df, ignore_fields, composite_key)
            report_df = select_report_fields(report_df, config.get("reportfields", []))

            report_csv = report_df.to_csv(index=False)

            report_key = f"reports/{batch_prefix}report.csv"
            upload_report(bucket, report_key, report_csv)
            LOG.info("Report written to s3://%s/%s", bucket, report_key)

        except ConfigError as exc:
            LOG.error("Config error: %s", exc)
        except Exception as exc:
            LOG.exception("Unhandled error: %s", exc)

    return {"statusCode": 200, "body": "ok"}
