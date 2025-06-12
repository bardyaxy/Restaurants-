from tests import requests_stub
import restaurants.network_utils as network_utils


def test_check_network_success(monkeypatch):
    def dummy_get(url, timeout, allow_redirects):
        class Resp:
            pass

        return Resp()

    monkeypatch.setattr(requests_stub, "get", dummy_get)
    monkeypatch.setattr(network_utils, "requests", requests_stub)
    assert network_utils.check_network()


def test_check_network_failure(monkeypatch):
    def dummy_get(url, timeout, allow_redirects):
        raise requests_stub.RequestException

    monkeypatch.setattr(requests_stub, "get", dummy_get)
    monkeypatch.setattr(network_utils, "requests", requests_stub)
    assert not network_utils.check_network()


def test_check_network_head(monkeypatch):
    def dummy_head(url, timeout, allow_redirects):
        assert allow_redirects is False

        class Resp:
            pass

        return Resp()

    monkeypatch.setattr(requests_stub, "head", dummy_head)
    monkeypatch.setattr(network_utils, "requests", requests_stub)
    assert network_utils.check_network(method="HEAD")


def test_check_network_env_overrides(monkeypatch):
    """check_network should honor environment variable overrides."""

    called = {}

    def dummy_head(url, timeout, allow_redirects):
        called["url"] = url
        called["timeout"] = timeout
        called["allow_redirects"] = allow_redirects

        class Resp:
            pass

        return Resp()

    monkeypatch.setattr(requests_stub, "head", dummy_head)
    monkeypatch.setattr(network_utils, "requests", requests_stub)

    monkeypatch.setenv("NETWORK_TEST_URL", "https://example.com/ping")
    monkeypatch.setenv("NETWORK_TEST_METHOD", "HEAD")
    monkeypatch.setenv("NETWORK_TEST_TIMEOUT", "11")

    assert network_utils.check_network()
    assert called == {
        "url": "https://example.com/ping",
        "timeout": 11,
        "allow_redirects": False,
    }


def test_check_network_invalid_env_method(monkeypatch):
    """Invalid NETWORK_TEST_METHOD should fall back to the default."""

    called = {}

    def dummy_get(url, timeout, allow_redirects):
        called["method"] = "GET"

        class Resp:
            pass

        return Resp()

    monkeypatch.setattr(requests_stub, "get", dummy_get)
    monkeypatch.setattr(network_utils, "requests", requests_stub)

    errors = []

    def dummy_error(msg, *args):
        errors.append(msg % args)

    monkeypatch.setattr(network_utils.logging, "error", dummy_error)

    monkeypatch.setenv("NETWORK_TEST_METHOD", "POST")

    assert network_utils.check_network()
    assert called["method"] == "GET"
    assert any("Invalid NETWORK_TEST_METHOD" in e for e in errors)
