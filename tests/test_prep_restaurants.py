import importlib
import pandas as pd
import glob
import os


def test_prep_restaurants_functions(tmp_path, monkeypatch):
    df = pd.DataFrame(
        {
            "Opening Hours": ["Mon: 9-5"],
            "lat": [47.6],
            "lon": [-122.2],
            "Price Level": [2],
            "Formatted Phone Number": ["123"],
            "Website": ["x"],
        }
    )

    monkeypatch.setattr(
        glob,
        "glob",
        lambda pattern: [str(tmp_path / "input.csv")],
    )
    monkeypatch.setattr(pd, "read_csv", lambda path: df)

    captured = {}

    def dummy_to_csv(self, path, index=False):
        captured["csv"] = path

    def dummy_to_excel(self, path, index=False, engine=None):
        captured["xlsx"] = path

    monkeypatch.setattr(pd.DataFrame, "to_csv", dummy_to_csv)
    monkeypatch.setattr(pd.DataFrame, "to_excel", dummy_to_excel)

    def dummy_replace(src, dst):
        captured.setdefault("replace", []).append((src, dst))

    monkeypatch.setattr(os, "replace", dummy_replace)

    pr = importlib.import_module("restaurants.prep_restaurants")
    pr.main()

    assert captured.get("csv") == "restaurants_prepped.tmp.csv"
    assert captured.get("xlsx") == "restaurants_prepped.tmp.xlsx"
    assert (
        "restaurants_prepped.tmp.csv",
        "restaurants_prepped.csv",
    ) in captured.get("replace", [])
    assert (
        "restaurants_prepped.tmp.xlsx",
        "restaurants_prepped.xlsx",
    ) in captured.get("replace", [])
    assert pr.split_hours("Mon: 9-5; Tue: 10-6") == {
        "Mon": "9-5",
        "Tue": "10-6",
    }
    assert (
        pr._bx_distance(pd.Series({"lat": pr.BX_LAT, "lon": pr.BX_LON})) == 0
    )

    series_dist = pr.haversine_miles_series(
        pd.Series([pr.BX_LAT]),
        pd.Series([pr.BX_LON]),
        pr.BX_LAT,
        pr.BX_LON,
    )
    assert series_dist.iloc[0] == 0
