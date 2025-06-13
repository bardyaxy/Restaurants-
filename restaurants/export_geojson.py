#!/usr/bin/env python3
"""Convert restaurants_prepped.csv to a GeoJSON FeatureCollection."""

from __future__ import annotations

import json
from pathlib import Path
import logging

import pandas as pd


def main(argv: list[str] | None = None) -> None:
    """Read the CSV and write ``backend/static/restaurants.geojson``."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    csv_path = Path("restaurants_prepped.csv")
    if not csv_path.exists():
        raise SystemExit(f"Missing {csv_path}")

    df = pd.read_csv(csv_path)
    features: list[dict] = []

    for _, row in df.iterrows():
        lat = row.get("lat")
        lon = row.get("lon")
        if pd.isna(lat) or pd.isna(lon):
            continue
        props = row.drop(labels=["lat", "lon"]).to_dict()
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(lon), float(lat)],
            },
            "properties": props,
        }
        features.append(feature)

    geojson = {"type": "FeatureCollection", "features": features}

    out_dir = Path("backend/static")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "restaurants.geojson"
    out_path.write_text(json.dumps(geojson, indent=2))
    logging.info("Wrote %s with %s features", out_path, len(features))


if __name__ == "__main__":  # pragma: no cover - manual execution
    main()
