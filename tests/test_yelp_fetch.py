import os
import json
import importlib
from datetime import datetime
from pathlib import Path

import pytest


def test_search_yelp_businesses_paginates(monkeypatch):
    os.environ["YELP_API_KEY"] = "TEST"
    yf = importlib.import_module("restaurants.yelp_fetch")

    calls = []

    class DummyResp:
        def __init__(self, data):
            self._data = data

        @staticmethod
        def raise_for_status():
            pass

        def json(self):
            return self._data

    class DummySession:
        def get(self, url, headers=None, params=None, timeout=None):
            calls.append(params.get("offset"))
            if params.get("offset") == 0:
                return DummyResp({"businesses": [{"id": "a"}, {"id": "b"}], "total": 3})
            elif params.get("offset") == 2:
                return DummyResp({"businesses": [{"id": "c"}], "total": 3})
            else:
                return DummyResp({"businesses": [], "total": 3})

    session = DummySession()
    results = yf.search_yelp_businesses("98501", session, limit=2)
    assert [b["id"] for b in results] == ["a", "b", "c"]
    assert calls == [0, 2]


def test_cli_writes_json(tmp_path, monkeypatch):
    os.environ["YELP_API_KEY"] = "TEST"
    yf = importlib.import_module("restaurants.yelp_fetch")

    monkeypatch.setattr(yf, "enrich_restaurants", lambda zip: [{"id": "x"}])
    monkeypatch.setattr(yf, "setup_logging", lambda *a, **kw: None)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls):
            return cls(2023, 1, 2, 3, 4, 5)

    monkeypatch.setattr(yf, "datetime", FixedDatetime)

    monkeypatch.chdir(tmp_path)
    yf.main(["12345"])

    files = list(Path(tmp_path).glob("yelp_businesses_12345_*.json"))
    assert len(files) == 1
    with files[0].open() as f:
        data = json.load(f)
    assert data == [{"id": "x"}]

