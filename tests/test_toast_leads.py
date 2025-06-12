import restaurants.toast_leads as tl
import pathlib


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


def test_load_zip_codes_validation(tmp_path):
    path = tmp_path / "zips.txt"
    path.write_text("98101\nabcde\n12345-6789\n")
    assert tl.load_zip_codes(path) == ["98101", "12345-6789"]


def test_toast_leads_chain_blocklist(monkeypatch, tmp_path):
    zip_file = tmp_path / "zips.txt"
    zip_file.write_text("98501\n")
    monkeypatch.setattr(tl, "ZIP_FILE", str(zip_file))
    monkeypatch.setattr(tl, "check_network", lambda: True)
    monkeypatch.setattr(tl, "load_seen_ids", lambda path=None: set())
    monkeypatch.setattr(tl, "save_seen_ids", lambda ids, path=None: None)

    captured_rows = []

    class CapturingWriter:
        def __init__(self, f, fieldnames):
            self.fieldnames = fieldnames

        def writeheader(self):
            pass

        def writerows(self, rows):
            captured_rows.extend(rows)

    monkeypatch.setattr(tl.csv, "DictWriter", CapturingWriter)

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
                "results": [
                    {"name": "IHOP West", "place_id": "p1"},
                    {"name": "Local Spot", "place_id": "p2"},
                ]
            })
        elif "details" in url:
            if params["place_id"] == "p1":
                return DummyResp({"result": {"name": "IHOP West"}})
            return DummyResp({"result": {"name": "Local Spot"}})
        raise AssertionError("unexpected url " + url)

    class DummySession:
        def __init__(self):
            self.trust_env = True

        def get(self, url, params=None, timeout=None):
            return dummy_get(self, url, params, timeout)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

    monkeypatch.setattr(tl.requests, "Session", DummySession)

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

    monkeypatch.setattr(tl, "ThreadPoolExecutor", DummyExecutor)
    monkeypatch.setattr(tl, "as_completed", lambda it: it)

    tl.main()

    assert len(captured_rows) == 1
    assert captured_rows[0]["Business Name"] == "Local Spot"

