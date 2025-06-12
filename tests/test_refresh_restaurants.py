import os
import pandas as pd
import pytest

os.environ.setdefault("GOOGLE_API_KEY", "DUMMY")

from restaurants import refresh_restaurants as rr
from restaurants.fetchers import google_places as gp


def test_google_details_use_threadpool(monkeypatch):
    monkeypatch.setattr(gp, "check_network", lambda: True)

    class DummyResp:
        def __init__(self, data):
            self._data = data
            self.status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return self._data

    detail_calls = []

    def dummy_get(self, url, params=None, timeout=None):
        if "textsearch" in url:
            return DummyResp(
                {
                    "results": [
                        {
                            "name": "A",
                            "formatted_address": "addr1",
                            "place_id": "p1",
                            "rating": 4.0,
                            "user_ratings_total": 5,
                            "business_status": "OP",
                            "geometry": {"location": {"lat": 1, "lng": 2}},
                        },
                        {
                            "name": "B",
                            "formatted_address": "addr2",
                            "place_id": "p2",
                            "rating": 3.5,
                            "user_ratings_total": 2,
                            "business_status": "OP",
                            "geometry": {"location": {"lat": 3, "lng": 4}},
                        },
                    ]
                }
            )
        elif "details" in url:
            detail_calls.append(params["place_id"])
            return DummyResp({"result": {}})
        else:
            raise AssertionError("unexpected url " + url)

    monkeypatch.setattr(gp.requests.sessions.Session, "get", dummy_get)

    executors = []

    class DummyFuture:
        def __init__(self, res):
            self._res = res

        def result(self):
            return self._res

    class DummyExecutor:
        def __init__(self, max_workers=None):
            executors.append(self)
            self.submitted = []

        def submit(self, fn, *args, **kw):
            res = fn(*args, **kw)
            fut = DummyFuture(res)
            self.submitted.append((fn, args, kw))
            return fut

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

    def dummy_as_completed(iterable):
        for f in iterable:
            yield f

    monkeypatch.setattr(gp, "ThreadPoolExecutor", DummyExecutor)
    monkeypatch.setattr(gp, "as_completed", dummy_as_completed)

    rr.smb_restaurants_data.clear()
    gp.GooglePlacesFetcher().fetch(["98501"])

    assert executors
    assert len(executors[0].submitted) == 2
    assert detail_calls == ["p1", "p2"]


def test_fetch_google_places_no_network(monkeypatch):
    monkeypatch.setattr(gp, "check_network", lambda: False)
    with pytest.raises(SystemExit):
        gp.GooglePlacesFetcher().fetch(["98501"])


def test_fetch_google_places_textsearch_error(monkeypatch):
    monkeypatch.setattr(gp, "check_network", lambda: True)

    class DummySessionGet:
        def __call__(self, url, params=None, timeout=None):
            if "textsearch" in url:
                raise gp.requests.RequestException("boom")
            raise AssertionError("unexpected url " + url)

    monkeypatch.setattr(gp.requests.sessions.Session, "get", DummySessionGet())

    with pytest.raises(SystemExit):
        gp.GooglePlacesFetcher().fetch(["98501"])


def test_fetch_google_places_details_failure(monkeypatch):
    monkeypatch.setattr(gp, "check_network", lambda: True)

    class DummyResp:
        def __init__(self, data):
            self._data = data
            self.status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return self._data

    def dummy_get(self, url, params=None, timeout=None):
        if "textsearch" in url:
            return DummyResp(
                {
                    "results": [
                        {
                            "name": "A",
                            "formatted_address": "addr1",
                            "place_id": "p1",
                            "rating": 4.0,
                            "user_ratings_total": 1,
                            "business_status": "OP",
                            "geometry": {"location": {"lat": 1, "lng": 2}},
                        }
                    ]
                }
            )
        elif "details" in url:
            raise RuntimeError("boom")
        raise AssertionError("unexpected url " + url)

    monkeypatch.setattr(gp.requests.sessions.Session, "get", dummy_get)
    monkeypatch.setattr(gp.time, "sleep", lambda _x: None)

    class DummyFuture:
        def __init__(self, res):
            self._res = res

        def result(self):
            return self._res

    class DummyExecutor:
        def __init__(self, max_workers=None):
            pass

        def submit(self, fn, *a, **kw):
            return DummyFuture(fn(*a, **kw))

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

    monkeypatch.setattr(gp, "ThreadPoolExecutor", DummyExecutor)
    monkeypatch.setattr(gp, "as_completed", lambda it: it)

    with pytest.raises(SystemExit):
        gp.GooglePlacesFetcher().fetch(["98501"])


def test_fetch_google_places_chain_blocklist(monkeypatch):
    monkeypatch.setattr(gp, "check_network", lambda: True)

    class DummyResp:
        def __init__(self, data):
            self._data = data
            self.status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return self._data

    def dummy_get(self, url, params=None, timeout=None):
        if "textsearch" in url:
            return DummyResp(
                {
                    "results": [
                        {
                            "name": "Denny's",
                            "formatted_address": "addr1",
                            "place_id": "p1",
                            "rating": 3.0,
                            "user_ratings_total": 1,
                            "business_status": "OP",
                            "geometry": {"location": {"lat": 1, "lng": 2}},
                        },
                        {
                            "name": "Local Cafe",
                            "formatted_address": "addr2",
                            "place_id": "p2",
                            "rating": 4.5,
                            "user_ratings_total": 7,
                            "business_status": "OP",
                            "geometry": {"location": {"lat": 3, "lng": 4}},
                        },
                    ]
                }
            )
        elif "details" in url:
            return DummyResp({"result": {}})
        raise AssertionError("unexpected url " + url)

    monkeypatch.setattr(gp.requests.sessions.Session, "get", dummy_get)

    class DummyFuture:
        def __init__(self, res):
            self._res = res

        def result(self):
            return self._res

    class DummyExecutor:
        def __init__(self, max_workers=None):
            pass

        def submit(self, fn, *args, **kw):
            return DummyFuture(fn(*args, **kw))

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

    monkeypatch.setattr(gp, "ThreadPoolExecutor", DummyExecutor)
    monkeypatch.setattr(gp, "as_completed", lambda it: it)

    rows = gp.GooglePlacesFetcher().fetch(["98501"])
    assert len(rows) == 1
    assert rows[0]["Name"] == "Local Cafe"


def test_main_missing_api_key(monkeypatch):
    monkeypatch.setattr(rr, "GOOGLE_API_KEY", None)
    monkeypatch.setattr(rr, "FETCHERS", [])
    with pytest.raises(SystemExit):
        rr.main(["--zips", "98501"])


def test_strict_zips_filters_rows(monkeypatch):
    class DummyFetcher:
        def fetch(self, zip_codes, **opts):
            return [
                {"Name": "A", "Zip Code": "98501"},
                {"Name": "B", "Zip Code": "99999"},
                {"Name": "C", "Zip Code": "98002"},
            ]

    monkeypatch.setattr(rr, "FETCHERS", [(DummyFetcher, True)])
    monkeypatch.setattr(rr, "GOOGLE_API_KEY", "DUMMY")
    monkeypatch.setattr(rr.loader, "load", lambda _p: None)
    monkeypatch.setattr(rr.pd, "read_sql_query", lambda q, c: pd.DataFrame())

    class DummyConn:
        def close(self):
            pass

    monkeypatch.setattr(rr.sqlite3, "connect", lambda _p: DummyConn())

    saved = []

    def dummy_to_csv(self, path, index=False):
        saved.append(self.copy())

    monkeypatch.setattr(pd.DataFrame, "to_csv", dummy_to_csv)

    rr.main(["--zips", "98501,98002", "--strict-zips"])

    assert saved
    df = saved[0]
    assert list(df["Zip Code"]) == ["98501", "98002"]
