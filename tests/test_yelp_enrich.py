import os
import sqlite3
import pytest

pytest.importorskip("rapidfuzz")


def test_enrich_exits_without_network(tmp_path, monkeypatch):
    os.environ["YELP_API_KEY"] = "TEST"
    os.environ.setdefault("GOOGLE_API_KEY", "DUMMY")
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
    os.environ.setdefault("GOOGLE_API_KEY", "DUMMY")
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

    called_params = {}

    def dummy_get(url, headers, params, timeout):
        called_params.update(params)
        class Resp:
            @staticmethod
            def raise_for_status():
                pass

            @staticmethod
            def json():
                return {
                    "businesses": [
                        {
                            "name": "Foo",
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
        "SELECT yelp_cuisines, yelp_primary_cuisine, yelp_status "
        "FROM places WHERE place_id='pid1'"
    ).fetchone()
    conn.close()
    assert row == ("pizza,italian", "pizza", "SUCCESS")
    assert called_params.get("limit") == 5


def test_enrich_selects_best_match(tmp_path, monkeypatch):
    os.environ["YELP_API_KEY"] = "TEST"
    os.environ.setdefault("GOOGLE_API_KEY", "DUMMY")
    from restaurants import loader, yelp_enrich

    tmp_db = tmp_path / "dela.sqlite"
    monkeypatch.setattr(loader, "DB_PATH", tmp_db)
    conn = loader.ensure_db()
    conn.execute(
        "INSERT INTO places (place_id, name, city, state) VALUES (?,?,?,?)",
        ("pid1", "Foo Bar", "Olympia", "WA"),
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
                            "name": "Completely Different",
                            "rating": 3.0,
                            "review_count": 5,
                            "price": "$",
                            "categories": [],
                        },
                        {
                            "name": "Foo Bar",
                            "rating": 4.5,
                            "review_count": 10,
                            "price": "$$",
                            "categories": [],
                        },
                    ]
                }

        return Resp()

    monkeypatch.setattr(yelp_enrich.requests, "get", dummy_get)

    yelp_enrich.enrich()

    row = sqlite3.connect(tmp_db).execute(
        "SELECT yelp_rating, yelp_status FROM places WHERE place_id='pid1'"
    ).fetchone()
    assert row == (4.5, "SUCCESS")


def test_enrich_no_candidate_above_threshold(tmp_path, monkeypatch):
    os.environ["YELP_API_KEY"] = "TEST"
    os.environ.setdefault("GOOGLE_API_KEY", "DUMMY")
    from restaurants import loader, yelp_enrich

    tmp_db = tmp_path / "dela.sqlite"
    monkeypatch.setattr(loader, "DB_PATH", tmp_db)
    conn = loader.ensure_db()
    conn.execute(
        "INSERT INTO places (place_id, name, city, state) VALUES (?,?,?,?)",
        ("pid2", "Another", "Olympia", "WA"),
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
                            "name": "Not Even Close",
                            "rating": 3.0,
                            "review_count": 2,
                            "price": "$",
                            "categories": [],
                        }
                    ]
                }

        return Resp()

    monkeypatch.setattr(yelp_enrich.requests, "get", dummy_get)

    yelp_enrich.enrich()

    row = sqlite3.connect(tmp_db).execute(
        "SELECT yelp_status FROM places WHERE place_id='pid2'"
    ).fetchone()
    assert row == ("FAIL",)


def test_enrich_retries_missing_categories(tmp_path, monkeypatch):
    """Rows lacking cuisines should be reprocessed even if yelp_status is set."""
    os.environ["YELP_API_KEY"] = "TEST"
    os.environ.setdefault("GOOGLE_API_KEY", "DUMMY")
    from restaurants import loader, yelp_enrich

    tmp_db = tmp_path / "dela.sqlite"
    monkeypatch.setattr(loader, "DB_PATH", tmp_db)
    conn = loader.ensure_db()
    conn.execute(
        "INSERT INTO places (place_id, name, city, state, yelp_status) VALUES (?,?,?,?,?)",
        ("pid3", "Retry", "Olympia", "WA", "SUCCESS"),
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
                            "name": "Retry",
                            "rating": 4.0,
                            "review_count": 8,
                            "price": "$",
                            "categories": [{"alias": "thai", "title": "Thai"}],
                        }
                    ]
                }

        return Resp()

    monkeypatch.setattr(yelp_enrich.requests, "get", dummy_get)

    yelp_enrich.enrich()

    row = sqlite3.connect(tmp_db).execute(
        "SELECT yelp_cuisines, yelp_primary_cuisine FROM places WHERE place_id='pid3'"
    ).fetchone()
    assert row == ("thai", "thai")
