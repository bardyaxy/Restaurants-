import os
import importlib
import sqlite3
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
            return DummyResp(
                {
                    "results": [
                        {
                            "name": "Foo",
                            "place_id": "p1",
                            "geometry": {"location": {"lat": 1.0, "lng": 2.0}},
                        }
                    ]
                }
            )
        elif url == gye.GOOGLE_DETAILS_URL:
            return DummyResp({"result": {}})
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


def test_enrich_restaurant_phone_fallback(monkeypatch):
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
            return DummyResp(
                {
                    "results": [
                        {
                            "name": "Foo",
                            "place_id": "p1",
                            "geometry": {"location": {"lat": 1.0, "lng": 2.0}},
                        }
                    ]
                }
            )
        elif url == gye.GOOGLE_DETAILS_URL:
            return DummyResp(
                {"result": {"formatted_phone_number": "+1-555-111-2222"}}
            )
        elif url == gye.YELP_SEARCH_URL:
            return DummyResp({"businesses": []})
        elif url == gye.YELP_PHONE_SEARCH_URL:
            return DummyResp({"businesses": [{"id": "y1"}]})
        elif url == gye.YELP_DETAILS_URL.format(id="y1"):
            return DummyResp({"id": "y1"})
        elif url == gye.YELP_REVIEWS_URL.format(id="y1"):
            return DummyResp({"reviews": []})
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(gye.requests.sessions.Session, "get", dummy_get)
    monkeypatch.setattr(gye, "check_network", lambda: True)

    res = gye.enrich_restaurant("Foo", "Olympia WA")
    assert res["yelp"]["business"]["id"] == "y1"


def test_yelp_enrich_all_updates_db(tmp_path, monkeypatch):
    gye = importlib.import_module("restaurants.google_yelp_enrich")

    tmp_db = tmp_path / "dela.sqlite"
    monkeypatch.setattr(gye.loader, "DB_PATH", tmp_db)
    loader_mod = importlib.import_module("restaurants.loader")
    monkeypatch.setattr(loader_mod, "DB_PATH", tmp_db)
    gye.loader.ensure_db()
    conn = sqlite3.connect(tmp_db)
    conn.execute(
        "INSERT INTO places (place_id, name, city, state)"
        " VALUES ('p1','Foo','Olympia','WA')"
    )
    conn.commit()
    conn.close()

    def dummy_enrich(name, loc):
        return {
            "google": {},
            "yelp": {
                "business": {},
                "details": {
                    "categories": [{"alias": "thai", "title": "Thai"}]
                },
                "reviews": {},
                "summary": {
                    "rating": 4.5,
                    "review_count": 7,
                    "price": "$$",
                    "is_closed": False,
                },
            },
        }

    monkeypatch.setattr(gye, "enrich_restaurant", dummy_enrich)
    gye.yelp_enrich_all()

    conn = sqlite3.connect(tmp_db)
    row = conn.execute(
        "SELECT yelp_rating, yelp_reviews, yelp_price_tier, yelp_cuisines,"
        " yelp_primary_cuisine, yelp_category_titles, yelp_status"
        " FROM places WHERE place_id='p1'"
    ).fetchone()
    conn.close()
    assert row == (4.5, 7, "$$", "thai", "thai", "Thai", "open")
