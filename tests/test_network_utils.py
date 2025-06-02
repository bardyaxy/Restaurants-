import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import requests
import pytest
from network_utils import check_network


def test_check_network_success(monkeypatch):
    def dummy_head(url, timeout):
        class Resp:
            pass
        return Resp()

    monkeypatch.setattr(requests, "head", dummy_head)
    assert check_network()


def test_check_network_failure(monkeypatch):
    def dummy_head(url, timeout):
        raise requests.RequestException

    monkeypatch.setattr(requests, "head", dummy_head)
    assert not check_network()
