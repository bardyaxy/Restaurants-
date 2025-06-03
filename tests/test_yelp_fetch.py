import os
import json
import importlib
from datetime import datetime
from pathlib import Path
import requests

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

    def dummy_get(url, headers=None, params=None, timeout=None):
        calls.append(params.get("offset"))
        if params.get("offset") == 0:
            return DummyResp({"businesses": [{"id": "a"}, {"id": "b"}], "total": 3})
        elif params.get("offset") == 2:
            return DummyResp({"businesses": [{"id": "c"}], "total": 3})
        else:
            return DummyResp({"businesses": [], "total": 3})

    def dummy_session_get(self, url, headers=None, params=None, timeout=None):
        return dummy_get(url, headers=headers, params=params, timeout=timeout)

    monkeypatch.setattr(requests, "get", dummy_get)
    monkeypatch.setattr(requests.sessions.Session, "get", dummy_session_get)

    session = requests.Session()
    results = yf.search_yelp_businesses("98501", session, limit=2)
    assert [b["id"] for b in results] == ["a", "b", "c"]
    assert calls == [0, 2]


def test_enrich_restaurants_aggregates(monkeypatch):
    os.environ["YELP_API_KEY"] = "TEST"
    yf = importlib.import_module("restaurants.yelp_fetch")

    class DummyResp:
        def __init__(self, data):
            self._data = data

        @staticmethod
        def raise_for_status():
            pass

        def json(self):
            return self._data

    def dummy_get(url, headers=None, params=None, timeout=None):
        if url == yf.SEARCH_URL:
            return DummyResp({"businesses": [{"id": "x"}], "total": 1})
        if url == yf.DETAILS_URL.format(id="x"):
            return DummyResp({"id": "x", "name": "X"})
        if url == yf.REVIEWS_URL.format(id="x"):
            return DummyResp({"reviews": [{"id": "r"}]})
        raise AssertionError(f"unexpected url {url}")

    def dummy_session_get(self, url, headers=None, params=None, timeout=None):
        return dummy_get(url, headers=headers, params=params, timeout=timeout)

    monkeypatch.setattr(requests, "get", dummy_get)
    monkeypatch.setattr(requests.sessions.Session, "get", dummy_session_get)
    monkeypatch.setattr(yf, "check_network", lambda: True)

    results = yf.enrich_restaurants("98501")
    assert results == [
        {
            "business": {"id": "x"},
            "details": {"id": "x", "name": "X"},
            "reviews": {"reviews": [{"id": "r"}]},
        }
    ]


def test_cli_writes_json(tmp_path, monkeypatch):
    os.environ["YELP_API_KEY"] = "TEST"
    os.environ["YELP_ZIP"] = "12345"
    os.environ["YELP_OUT"] = str(tmp_path / "out.json")
    yf = importlib.import_module("restaurants.yelp_fetch")

    monkeypatch.setattr(yf, "enrich_restaurants", lambda zip_code: [{"id": zip_code}])
    monkeypatch.setattr(yf, "setup_logging", lambda *a, **kw: None)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls):
            return cls(2023, 1, 2, 3, 4, 5)

    monkeypatch.setattr(yf, "datetime", FixedDatetime)

    monkeypatch.chdir(tmp_path)
    yf.main([])

    with Path(os.environ["YELP_OUT"]).open() as f:
        data = json.load(f)
    assert data == [{"id": "12345"}]
