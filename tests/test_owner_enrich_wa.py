import asyncio
import pandas as pd
import pytest
import shutil

from restaurants import owner_enrich_wa as ow


@pytest.fixture(autouse=True)
def patch_cache_dir(monkeypatch, tmp_path):
    old = ow.CACHE_DIR
    if old.exists():
        shutil.rmtree(old)
    new_dir = tmp_path / "cache"
    new_dir.mkdir()
    monkeypatch.setattr(ow, "CACHE_DIR", new_dir)
    yield
    shutil.rmtree(new_dir, ignore_errors=True)


def test_build_url_quotes():
    url = ow._build_url("Bob's Burgers")
    assert "4wur-kfnr" in url
    assert "ILIKE" in url
    assert "Bob%27%27s+Burgers" in url


def test_enrich_state(monkeypatch):
    async def dummy_hit(session, name):
        return "123", f"owner-{name}"

    monkeypatch.setattr(ow, "_hit", dummy_hit)
    df = pd.DataFrame({"Name": ["Foo"]})
    res = asyncio.run(ow.enrich_state(df))
    assert res.loc[0, "ubi"] == "123"
    assert res.loc[0, "owner_name_state"] == "owner-Foo"
