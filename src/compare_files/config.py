import os
from typing import Dict

import boto3


class ConfigError(RuntimeError):
    pass


def get_config(feed_id: str) -> Dict:
    table_name = os.environ.get("DDB_TABLE", "CompareFileSchemaConfig")
    table = boto3.resource("dynamodb").Table(table_name)
    resp = table.get_item(Key={"feedId": feed_id})
    item = resp.get("Item")
    if not item:
        raise ConfigError(f"Missing config for feedId={feed_id}")

    item.setdefault("isoldfilecsv", True)
    item.setdefault("isnewfilecsv", True)
    if "fieldstocompare" not in item and "filedstocompare" in item:
        item["fieldstocompare"] = item["filedstocompare"]
    item.setdefault("fieldstocompare", [])
    item.setdefault("reportfields", [])
    item.setdefault("compositekey", [])
    return item
