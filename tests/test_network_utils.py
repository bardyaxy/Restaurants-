from tests import requests_stub
import pytest
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
