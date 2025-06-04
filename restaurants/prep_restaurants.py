#!/usr/bin/env python3
"""Clean Google SMB CSV and generate tidy outputs."""

from __future__ import annotations

import glob
import logging
import pandas as pd

try:
    from restaurants.utils import (
        haversine_miles,
        haversine_miles_series,
        setup_logging,
    )
except ImportError:  # pragma: no cover - fallback for running as script
    from utils import haversine_miles, haversine_miles_series, setup_logging  # type: ignore


BX_LAT, BX_LON = 47.6154255, -122.2035954  # Bellevue Square Mall


def split_hours(text: str) -> dict:
    """Parse semicolon-separated hours into a dictionary."""
    if pd.isna(text) or not text:
        return {}

    out: dict[str, str] = {}
    for segment in text.split(";"):
        if ":" not in segment:
            continue
        day, hours = segment.split(":", 1)
        out[day.strip()] = hours.strip()
    return out


def _bx_distance(row: pd.Series) -> float | None:
    dist = haversine_miles(row["lat"], row["lon"], BX_LAT, BX_LON)
    return round(dist, 2) if dist is not None else None


def main(argv: list[str] | None = None) -> None:
    """Entry point for cleaning the latest Google export."""

    setup_logging()

    # ------------------------------------------------------------------
    # 0.  Load the most-recent Google export
    # ------------------------------------------------------------------
    matches = sorted(glob.glob("olympia_smb_google_restaurants_*.csv"))
    if not matches:
        raise SystemExit("No olympia_smb_google_restaurants_*.csv files found")
    newest = matches[-1]
    df = pd.read_csv(newest)

    # ------------------------------------------------------------------
    # 1.  UTF-8 cleanup (narrow no-break space)
    # ------------------------------------------------------------------
    df["Opening Hours"] = (
        df["Opening Hours"].str.replace("\u202f", " ", regex=False)
    )

    # ------------------------------------------------------------------
    # 2.  Split opening hours into a dict per row
    # ------------------------------------------------------------------
    df["Opening Hours"] = df["Opening Hours"].apply(split_hours)

    # ------------------------------------------------------------------
    # 3.  Numeric price level -> $, $$, $$$ …
    # ------------------------------------------------------------------
    price_map = {0: "", 1: "$", 2: "$$", 3: "$$$", 4: "$$$$"}
    df["Price"] = df["Price Level"].map(price_map).fillna("")

    # ------------------------------------------------------------------
    # 4.  Haversine distance to Bellevue Square Mall
    # ------------------------------------------------------------------
    df["Distance Miles"] = (
        haversine_miles_series(df["lat"], df["lon"], BX_LAT, BX_LON).round(2)
    )

    # ------------------------------------------------------------------
    # 5.  Quick lead-quality flags
    # ------------------------------------------------------------------
    df["Has Phone"] = df["Formatted Phone Number"].str.len().gt(0).fillna(False)
    df["Has Website"] = df["Website"].str.len().gt(0).fillna(False)

    # ------------------------------------------------------------------
    # 6.  Drop bulky / duplicate columns
    # ------------------------------------------------------------------
    drop_cols = [
        "Photo Reference",
        "Types",
        "Price Level",
        "Street Address",
        "City",
        "State",
        "Zip Code",
    ]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    # ------------------------------------------------------------------
    # 7.  Save tidy outputs
    # ------------------------------------------------------------------
    out_csv = "restaurants_prepped.csv"
    out_xlsx = "restaurants_prepped.xlsx"

    df.to_csv(out_csv, index=False)
    df.to_excel(out_xlsx, index=False, engine="xlsxwriter")
    logging.info(
        "Cleaned %s → %s & %s  (%s rows)", newest, out_csv, out_xlsx, len(df)
    )


if __name__ == "__main__":  # pragma: no cover - manual execution
    main()

