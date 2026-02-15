from types import SimpleNamespace

import pytest

from compare_files.config import ConfigError, get_config


class FakeTable:
    def __init__(self, item):
        self._item = item

    def get_item(self, Key):
        return {"Item": self._item}


class FakeResource:
    def __init__(self, item):
        self._item = item

    def Table(self, name):
        return FakeTable(self._item)


def test_get_config_uses_feed_id_and_alias(monkeypatch):
    item = {
        "feedId": "teamA",
        "isoldfilecsv": True,
        "isnewfilecsv": False,
        "filedstocompare": ["ignore_me"],
        "reportfields": ["id"],
        "compositekey": ["id"],
    }

    def fake_resource(service):
        assert service == "dynamodb"
        return FakeResource(item)

    monkeypatch.setattr("boto3.resource", fake_resource)

    cfg = get_config("teamA")
    assert cfg["fieldstocompare"] == ["ignore_me"]
    assert cfg["isnewfilecsv"] is False


def test_get_config_missing_item(monkeypatch):
    def fake_resource(service):
        class EmptyTable:
            def get_item(self, Key):
                return {}

        class EmptyResource:
            def Table(self, name):
                return EmptyTable()

        return EmptyResource()

    monkeypatch.setattr("boto3.resource", fake_resource)

    with pytest.raises(ConfigError):
        get_config("missing")
