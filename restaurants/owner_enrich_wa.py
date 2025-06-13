from __future__ import annotations

import asyncio
import aiohttp
import os
import json
import pathlib
from urllib.parse import quote_plus

import pandas as pd

WA_DATASET = "4wur-kfnr"
BASE = f"https://data.wa.gov/resource/{WA_DATASET}.json"
APP_TOKEN = os.getenv("WA_APP_TOKEN")
CITY_APP_TOKEN = os.getenv("CITY_APP_TOKEN")

CACHE_DIR = pathlib.Path(__file__).resolve().parents[1] / "raw_responses"
CACHE_DIR.mkdir(exist_ok=True)

CITY_DATASETS = {
    "OLYMPIA": "f5gn-bcv7",
    "TACOMA": "w5rk-wqk7",
    "LAKEWOOD": "x7mr-j3bn",
}


def _cache_file(prefix: str, name: str) -> pathlib.Path:
    key = quote_plus(name)
    return CACHE_DIR / f"{prefix}_{key}.json"


def _build_url(name: str) -> str:
    safe = name.replace("'", "''")
    where = quote_plus(f"business_name ILIKE '{safe}'")
    cols = (
        "business_name,unified_business_identifier,"
        "governing_people_1_full_name,governing_people_2_full_name,"
        "governing_people_3_full_name,governing_people_4_full_name,"
        "governing_people_5_full_name"
    )
    return f"{BASE}?$limit=1&$select={cols}&$where={where}"


async def _hit(
    session: aiohttp.ClientSession, name: str
) -> tuple[str | None, str | None]:
    cache = _cache_file("state", name)
    if cache.exists():
        data = json.loads(cache.read_text())
    else:
        url = _build_url(name)
        headers = {"X-App-Token": APP_TOKEN} if APP_TOKEN else {}
        async with session.get(url, headers=headers, timeout=20) as resp:
            resp.raise_for_status()
            data = await resp.json()
        cache.write_text(json.dumps(data))
    if data:
        rec = data[0]
        owners = [
            rec.get(f"governing_people_{i}_full_name")
            for i in range(1, 6)
        ]
        owners = [o for o in owners if o]
        return (
            rec.get("unified_business_identifier"),
            owners[0] if owners else None,
        )
    return None, None


def _city_url(city: str, name: str) -> str:
    ds = CITY_DATASETS[city.upper()]
    safe = name.replace("'", "''")
    where = quote_plus(f"business_name ILIKE '{safe}'")
    return (
        f"https://data.{city.lower()}wa.gov/resource/{ds}.json?$limit=1"
        f"&$where={where}"
    )


def _extract_owner(rec: dict[str, str]) -> str | None:
    for k, v in rec.items():
        lk = k.lower()
        if "owner" in lk and "name" in lk:
            return v
    return None


async def _hit_city(
    session: aiohttp.ClientSession, city: str, name: str
) -> str | None:
    cache = _cache_file(city.lower(), name)
    if cache.exists():
        data = json.loads(cache.read_text())
    else:
        url = _city_url(city, name)
        headers = {"X-App-Token": CITY_APP_TOKEN} if CITY_APP_TOKEN else {}
        async with session.get(url, headers=headers, timeout=20) as resp:
            if resp.status == 200:
                data = await resp.json()
            else:
                data = []
        cache.write_text(json.dumps(data))
    if data:
        owner = _extract_owner(data[0])
        return owner
    return None


async def enrich_state(df: pd.DataFrame) -> pd.DataFrame:
    async with aiohttp.ClientSession() as session:
        tasks = [_hit(session, n) for n in df["Name"]]
        results = await asyncio.gather(*tasks)
    df["ubi"], df["owner_name_state"] = zip(*results)
    return df


async def enrich_cities(df: pd.DataFrame) -> pd.DataFrame:
    async with aiohttp.ClientSession() as session:
        tasks = []
        for _, row in df.iterrows():
            city = str(row.get("City", ""))
            name = str(row.get("Name", ""))
            if city.upper() in CITY_DATASETS and name:
                tasks.append(_hit_city(session, city, name))
            else:
                tasks.append(asyncio.sleep(0, None))
        owner_names = await asyncio.gather(*tasks)
    df["owner_name_city"] = owner_names
    return df
