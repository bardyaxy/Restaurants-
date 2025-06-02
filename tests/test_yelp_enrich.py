import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import os
import sqlite3


def test_enrich_exits_without_network(tmp_path, monkeypatch):
    os.environ["YELP_API_KEY"] = "TEST"
    import yelp_enrich
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
