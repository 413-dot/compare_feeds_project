# compare_files_project

## Requirements implemented
1. Files are uploaded to an existing S3 bucket by an external provider.
2. Validate that exactly two data files exist under the same batch prefix. Skip if count != 2, empty, or invalid.
3. Load schema/config from DynamoDB by `key` (feed identifier).
4. If a file is not CSV, convert JSON (array or JSON Lines) to CSV in-memory.
5. Ignore fields listed in DynamoDB `filedstocompare` (also accepts `fieldstocompare`).
6. Compare allowed columns using a composite key and produce a report CSV.
7. Write the report back to the same S3 bucket under `reports/<batch_prefix>/report.csv`.
8. Log errors and handle missing config, empty inputs, and missing composite keys.

## DynamoDB schema
Partition key: `feedId` (String)

Fields:
- `feedId`: Identify the feed / provider.
- `isoldfilecsv`: `true` if the old file is CSV, else JSON.
- `isnewfilecsv`: `true` if the new file is CSV, else JSON.
- `filedstocompare`: Fields to ignore for comparison.
- `reportfields`: Columns to include in the final report.
- `compositekey`: Columns used as a composite key to match rows.

Example item (both `filedstocompare` and `fieldstocompare` are accepted):
```
{
  "feedId": "teamA",
  "isoldfilecsv": true,
  "isnewfilecsv": false,
  "filedstocompare": ["updated_at", "checksum"],
  "reportfields": ["id", "name", "amount"],
  "compositekey": ["id", "sub_id"]
}
```

## S3 layout
Upload two files under the same batch prefix:
```
teamA/2026-02-11/batch-001/old_file.csv
teamA/2026-02-11/batch-001/new_file.json
```
The function writes a report to:
```
reports/teamA/2026-02-11/batch-001/report.csv
```

## Template structure
Source templates are split by service under `templates/` and can be merged into a single file for deployment.

Notes:
- `template.yaml` is a convenience default name. You can name the merged template anything and pass it to your deploy tool.
- The merge script is optional. If you prefer, you can maintain a single template file manually and delete the script.

Merge command (optional):
```
python scripts/merge_templates.py --base templates/base.yaml --fragments templates/lambda.yaml templates/dynamodb.yaml templates/outputs.yaml --out template.yaml
```

## Files and purpose
Templates (optional split):
- `templates/base.yaml`: Core parameters and template metadata.
- `templates/lambda.yaml`: Lambda definition, permissions, and S3 event trigger.
- `templates/dynamodb.yaml`: DynamoDB table for schema/config.
- `templates/outputs.yaml`: Stack outputs.
- `template.yaml`: Optional merged template used for deployment (name not mandatory).

Lambda code:
- `src/lambda_function.py`: Lambda handler (entrypoint).
- `src/compare_files/config.py`: DynamoDB config loader and normalization.
- `src/compare_files/s3_io.py`: S3 read/write helpers.
- `src/compare_files/normalize.py`: CSV/JSON normalization into pandas DataFrames.
- `src/compare_files/compare.py`: DataFrame comparison and diff generation.
- `src/compare_files/report.py`: Report column selection.
- `src/compare_files/__init__.py`: Package marker.

Tooling (optional):
- `scripts/merge_templates.py`: Merge template fragments.
- `scripts/requirements.txt`: Dependencies for the merge script.

Dependencies:
- `src/requirements.txt`: Lambda dependencies (`boto3`, `pandas`).

## Notes
- Each provider uses its own existing input bucket.
- The report is written back to the same bucket as the input files.
