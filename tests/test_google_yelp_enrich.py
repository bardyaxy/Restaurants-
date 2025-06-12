import os
import importlib
import pytest

os.environ.setdefault("GOOGLE_API_KEY", "DUMMY")
os.environ.setdefault("YELP_API_KEY", "DUMMY")


def test_enrich_restaurant_success(monkeypatch):
    gye = importlib.import_module("restaurants.google_yelp_enrich")

    class DummyResp:
        def __init__(self, data):
            self._data = data

        def raise_for_status(self):
            pass

        def json(self):
            return self._data

    def dummy_get(self, url, params=None, headers=None, timeout=None):
        if url == gye.GOOGLE_SEARCH_URL:
            return DummyResp({"results": [{"name": "Foo", "place_id": "p1", "geometry": {"location": {"lat": 1.0, "lng": 2.0}}}]})
        elif url == gye.YELP_SEARCH_URL:
            return DummyResp({"businesses": [{"id": "y1", "name": "Foo"}]})
        elif url == gye.YELP_DETAILS_URL.format(id="y1"):
            return DummyResp({"id": "y1", "name": "Foo Yelp"})
        elif url == gye.YELP_REVIEWS_URL.format(id="y1"):
            return DummyResp({"reviews": [{"id": "r1"}]})
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(gye.requests.sessions.Session, "get", dummy_get)
    monkeypatch.setattr(gye, "check_network", lambda: True)

    res = gye.enrich_restaurant("Foo", "Olympia WA")
    assert res["google"]["place_id"] == "p1"
    assert res["yelp"]["business"]["id"] == "y1"
    assert res["yelp"]["details"]["name"] == "Foo Yelp"
    assert res["yelp"]["reviews"]["reviews"][0]["id"] == "r1"
    assert res["yelp"]["summary"]["website"] is None


def test_enrich_restaurant_no_network(monkeypatch):
    gye = importlib.import_module("restaurants.google_yelp_enrich")
    monkeypatch.setattr(gye, "check_network", lambda: False)
    with pytest.raises(SystemExit):
        gye.enrich_restaurant("Foo", "Olympia WA")
