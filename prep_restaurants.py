#!/usr/bin/env python3
"""
Clean Google SMB CSV and generate:
  • restaurants_prepped.csv   (UTF-8 CSV)
  • restaurants_prepped.xlsx  (Excel, pivot-ready)
"""

import glob
import math
import pandas as pd

# ---------------------------------------------------------------------
# 0.  Load the most-recent Google export
# ---------------------------------------------------------------------
matches = sorted(glob.glob("olympia_smb_google_restaurants_*.csv"))
if not matches:
    raise SystemExit("No olympia_smb_google_restaurants_*.csv files found")
newest = matches[-1]
df = pd.read_csv(newest)

# ---------------------------------------------------------------------
# 1.  UTF-8 cleanup (narrow no-break space & en-dash)
# ---------------------------------------------------------------------
df["Opening Hours"] = (
    df["Opening Hours"]
      .str.replace("\u202f", " ", regex=False)   # NARROW NO-BREAK SPACE → space
      .str.replace("\u2013", "-", regex=False)   # EN DASH → hyphen
)

# ---------------------------------------------------------------------
# 2.  Split opening hours into a dict per row
# ---------------------------------------------------------------------
def split_hours(text: str) -> dict:
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

def haversine_miles(lat1, lon1, lat2=BX_LAT, lon2=BX_LON):
    """Great-circle distance in miles."""
    R = 3959
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    dφ  = math.radians(lat2 - lat1)
    dλ  = math.radians(lon2 - lon1)
    a = math.sin(dφ/2)**2 + math.cos(φ1)*math.cos(φ2)*math.sin(dλ/2)**2
    return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))

df["Distance Miles"] = df.apply(
    lambda r: round(haversine_miles(r["lat"], r["lon"]), 2), axis=1
)

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
