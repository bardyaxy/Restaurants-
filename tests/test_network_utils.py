import requests
import pytest
from restaurants.network_utils import check_network


def test_check_network_success(monkeypatch):
    def dummy_get(url, timeout, allow_redirects):
        class Resp:
            pass

        return Resp()

    monkeypatch.setattr(requests, "get", dummy_get)
    assert check_network()


def test_check_network_failure(monkeypatch):
    def dummy_get(url, timeout, allow_redirects):
        raise requests.RequestException

    monkeypatch.setattr(requests, "get", dummy_get)
    assert not check_network()


def test_check_network_head(monkeypatch):
    def dummy_head(url, timeout):
        class Resp:
            pass

        return Resp()

    monkeypatch.setattr(requests, "head", dummy_head)
    assert check_network(method="HEAD")
