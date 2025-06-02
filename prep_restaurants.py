import pandas as pd, re, json, glob


# pick the newest Google SMB CSV
newest = sorted(glob.glob("olympia_smb_google_restaurants_*.csv"))[-1]

df = pd.read_csv(newest)

# 1. Fix UTF-8 fancy chars -> ASCII
df["Opening Hours"] = (
    df["Opening Hours"].str.replace("\u202f", " ", regex=False)
    .str.replace("\u2013", "-", regex=False)
)


def split_hours(s: str) -> dict:
    out: dict[str, str] = {}
    for part in s.split(";"):
        if ":" not in part:
            continue
        day, hours = part.split(":", 1)
        out[day.strip()] = hours.strip()
    return out


df["Opening Hours"] = df["Opening Hours"].apply(split_hours)

# 3. Numeric price -> $, $$, $$$
price_map = {0: "", 1: "$", 2: "$$", 3: "$$$", 4: "$$$$"}
df["Price"] = df["Price Level"].map(price_map).fillna("")

# 4. Drop bulky columns you don't need
df = df.drop(columns=["Photo Reference", "Types", "Price Level"])

# 5. Save tidy version
out_csv = "restaurants_prepped.csv"
df.to_csv(out_csv, index=False)
print(f"✅ Wrote {out_csv} with {len(df)} rows")
