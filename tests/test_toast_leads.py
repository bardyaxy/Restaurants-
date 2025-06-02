import restaurants.toast_leads as tl


def test_load_seen_ids_missing(tmp_path):
    path = tmp_path / "seen.json"
    assert tl.load_seen_ids(path) == set()


def test_save_and_load_seen_ids_roundtrip(tmp_path):
    path = tmp_path / "seen.json"
    data = {"a", "b"}
    tl.save_seen_ids(data, path)
    assert tl.load_seen_ids(path) == data


def test_fetch_details_success(monkeypatch):
    class DummyResp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"result": {"name": "Foo"}}

    class DummySession:
        def get(self, url, params=None, timeout=None):
            return DummyResp()

    assert tl.fetch_details("pid", DummySession()) == {"name": "Foo"}


def test_fetch_details_error(monkeypatch):
    class DummySession:
        def get(self, url, params=None, timeout=None):
            raise RuntimeError("boom")

    assert tl.fetch_details("pid", DummySession()) == {}

