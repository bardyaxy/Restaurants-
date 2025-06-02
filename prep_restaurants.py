#!/usr/bin/env python3
"""
Clean Google SMB CSV and generate:
  • restaurants_prepped.csv   (UTF-8 CSV)
  • restaurants_prepped.xlsx  (Excel, pivot-ready)
"""

import glob
import pandas as pd
from utils import haversine_miles

# ---------------------------------------------------------------------
# 0.  Load the most-recent Google export
# ---------------------------------------------------------------------
matches = sorted(glob.glob("olympia_smb_google_restaurants_*.csv"))
if not matches:
    raise SystemExit("No olympia_smb_google_restaurants_*.csv files found")
newest = matches[-1]
df = pd.read_csv(newest)

# ---------------------------------------------------------------------
# 1.  UTF-8 cleanup (narrow no-break space)
# ---------------------------------------------------------------------
df["Opening Hours"] = (
    df["Opening Hours"]
      .str.replace("\u202f", " ", regex=False)   # NARROW NO-BREAK SPACE → space
)

# ---------------------------------------------------------------------
# 2.  Split opening hours into a dict per row
# ---------------------------------------------------------------------
def split_hours(text: str) -> dict:
    """Parse semicolon-separated hours into a dictionary."""
    if pd.isna(text) or not text:
        return {}

    out = {}
    for segment in text.split(";"):
        if ":" not in segment:
            continue
        day, hours = segment.split(":", 1)
        out[day.strip()] = hours.strip()
    return out

df["Opening Hours"] = df["Opening Hours"].apply(split_hours)

# ---------------------------------------------------------------------
# 3.  Numeric price level → $, $$, $$$ …
# ---------------------------------------------------------------------
price_map = {0: "", 1: "$", 2: "$$", 3: "$$$", 4: "$$$$"}
df["Price"] = df["Price Level"].map(price_map).fillna("")

# ---------------------------------------------------------------------
# 4.  Haversine distance to Bellevue Square Mall
# ---------------------------------------------------------------------
BX_LAT, BX_LON = 47.6154255, -122.2035954      # Bellevue Square Mall

def _bx_distance(row):
    dist = haversine_miles(row["lat"], row["lon"], BX_LAT, BX_LON)
    return round(dist, 2) if dist is not None else None

df["Distance Miles"] = df.apply(_bx_distance, axis=1)

# ---------------------------------------------------------------------
# 5.  Quick lead-quality flags
# ---------------------------------------------------------------------
df["Has Phone"]   = df["Formatted Phone Number"].str.len().gt(0).fillna(False)
df["Has Website"] = df["Website"].str.len().gt(0).fillna(False)

# ---------------------------------------------------------------------
# 6.  Drop bulky / duplicate columns
# ---------------------------------------------------------------------
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

# ---------------------------------------------------------------------
# 7.  Save tidy outputs
# ---------------------------------------------------------------------
out_csv  = "restaurants_prepped.csv"
out_xlsx = "restaurants_prepped.xlsx"

df.to_csv(out_csv,  index=False)
df.to_excel(out_xlsx, index=False, engine="xlsxwriter")

print(f"✅ Cleaned {newest} → {out_csv} & {out_xlsx}  ({len(df)} rows)")
