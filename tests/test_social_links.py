from tests import requests_stub
from restaurants import social_links


def test_extract_social_links(monkeypatch):
    html = (
        "<a href='http://facebook.com/foo'>fb</a>"
        "<a href='https://instagram.com/bar'>ig</a>"
    )

    class DummyResp:
        text = html

    def dummy_get(url, timeout):
        return DummyResp()

    monkeypatch.setattr(requests_stub, "get", dummy_get)
    monkeypatch.setattr(social_links, "requests", requests_stub)

    links = social_links.extract_social_links("http://example.com")
    assert links == {
        "facebook_url": "http://facebook.com/foo",
        "instagram_url": "https://instagram.com/bar",
    }


def test_extract_social_links_error(monkeypatch):
    def dummy_get(url, timeout):
        raise requests_stub.RequestException

    monkeypatch.setattr(requests_stub, "get", dummy_get)
    monkeypatch.setattr(social_links, "requests", requests_stub)

    links = social_links.extract_social_links("http://example.com")
    assert links == {}
