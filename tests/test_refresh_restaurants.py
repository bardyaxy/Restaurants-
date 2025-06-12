import os
import pytest

os.environ.setdefault("GOOGLE_API_KEY", "DUMMY")

from restaurants import refresh_restaurants as rr


def test_google_details_use_threadpool(monkeypatch):
    monkeypatch.setattr(rr, "check_network", lambda: True)

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
            return DummyResp({
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
            })
        elif "details" in url:
            detail_calls.append(params["place_id"])
            return DummyResp({"result": {}})
        else:
            raise AssertionError("unexpected url " + url)

    monkeypatch.setattr(rr.requests.sessions.Session, "get", dummy_get)

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

    monkeypatch.setattr(rr, "ThreadPoolExecutor", DummyExecutor)
    monkeypatch.setattr(rr, "as_completed", dummy_as_completed)

    rr.smb_restaurants_data.clear()
    rr.fetch_google_places(["98501"])

    assert executors
    assert len(executors[0].submitted) == 2
    assert detail_calls == ["p1", "p2"]


def test_fetch_google_places_no_network(monkeypatch):
    monkeypatch.setattr(rr, "check_network", lambda: False)
    with pytest.raises(SystemExit):
        rr.fetch_google_places(["98501"])


def test_fetch_google_places_textsearch_error(monkeypatch):
    monkeypatch.setattr(rr, "check_network", lambda: True)

    class DummySessionGet:
        def __call__(self, url, params=None, timeout=None):
            if "textsearch" in url:
                raise rr.requests.RequestException("boom")
            raise AssertionError("unexpected url " + url)

    monkeypatch.setattr(rr.requests.sessions.Session, "get", DummySessionGet())

    with pytest.raises(SystemExit):
        rr.fetch_google_places(["98501"])


def test_fetch_google_places_details_failure(monkeypatch):
    monkeypatch.setattr(rr, "check_network", lambda: True)

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
            return DummyResp({
                "results": [{
                    "name": "A",
                    "formatted_address": "addr1",
                    "place_id": "p1",
                    "rating": 4.0,
                    "user_ratings_total": 1,
                    "business_status": "OP",
                    "geometry": {"location": {"lat": 1, "lng": 2}},
                }]
            })
        elif "details" in url:
            raise RuntimeError("boom")
        raise AssertionError("unexpected url " + url)

    monkeypatch.setattr(rr.requests.sessions.Session, "get", dummy_get)
    monkeypatch.setattr(rr.time, "sleep", lambda _x: None)

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

    monkeypatch.setattr(rr, "ThreadPoolExecutor", DummyExecutor)
    monkeypatch.setattr(rr, "as_completed", lambda it: it)

    with pytest.raises(SystemExit):
        rr.fetch_google_places(["98501"])


def test_main_missing_api_key(monkeypatch):
    monkeypatch.setattr(rr, "GOOGLE_API_KEY", None)
    monkeypatch.setattr(rr, "fetch_google_places", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError))
    with pytest.raises(SystemExit):
        rr.main(["--zips", "98501"])

