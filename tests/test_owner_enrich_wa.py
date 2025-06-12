import asyncio
import pandas as pd

from restaurants import owner_enrich_wa as ow


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

