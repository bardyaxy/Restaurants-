import os
import sqlite3


def test_enrich_exits_without_network(tmp_path, monkeypatch):
    os.environ["YELP_API_KEY"] = "TEST"
    from restaurants import yelp_enrich
    tmp_db = tmp_path / "dela.sqlite"
    tmp_db.touch()
    monkeypatch.setattr(yelp_enrich, "DB_PATH", tmp_db)
    monkeypatch.setattr(yelp_enrich, "check_network", lambda: False)

    called = []
    def dummy_connect(path):
        called.append(path)
        raise AssertionError("connect should not be called")
    monkeypatch.setattr(sqlite3, "connect", dummy_connect)

    yelp_enrich.enrich()
    assert called == []


def test_enrich_inserts_categories(tmp_path, monkeypatch):
    os.environ["YELP_API_KEY"] = "TEST"
    from restaurants import loader, yelp_enrich

    tmp_db = tmp_path / "dela.sqlite"
    monkeypatch.setattr(loader, "DB_PATH", tmp_db)
    conn = loader.ensure_db()
    conn.execute(
        "INSERT INTO places (place_id, name, city, state, lat, lon) VALUES (?,?,?,?,?,?)",
        ("pid1", "Foo", "Olympia", "WA", 47.0, -122.0),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(yelp_enrich, "DB_PATH", tmp_db)
    monkeypatch.setattr(yelp_enrich, "check_network", lambda: True)

    def dummy_get(url, headers, params, timeout):
        class Resp:
            @staticmethod
            def raise_for_status():
                pass

            @staticmethod
            def json():
                return {
                    "businesses": [
                        {
                            "rating": 4.0,
                            "review_count": 20,
                            "price": "$$",
                            "categories": [
                                {"alias": "pizza", "title": "Pizza"},
                                {"alias": "italian", "title": "Italian"},
                            ],
                        }
                    ]
                }

        return Resp()

    monkeypatch.setattr(yelp_enrich.requests, "get", dummy_get)

    yelp_enrich.enrich()

    conn = sqlite3.connect(tmp_db)
    row = conn.execute(
        "SELECT yelp_cuisines, yelp_primary_cuisine FROM places WHERE place_id='pid1'"
    ).fetchone()
    conn.close()
    assert row == ("pizza,italian", "pizza")
