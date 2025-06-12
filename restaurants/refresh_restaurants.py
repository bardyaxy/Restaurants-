"""Refresh restaurant information for the Olympia, WA area."""

from __future__ import annotations

import argparse
import logging
import pathlib
import sqlite3
from datetime import datetime

import pandas as pd

from restaurants.utils import setup_logging
from restaurants import loader
from restaurants.config import GOOGLE_API_KEY, TARGET_OLYMPIA_ZIPS
from restaurants.settings import FETCHERS

# Aggregate store for fetched restaurant rows
smb_restaurants_data: list[dict] = []


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Refresh restaurant data")
    parser.add_argument(
        "--zips",
        dest="zips",
        help="Comma-separated list of ZIP codes to query",
    )
    parser.add_argument(
        "--strict-zips",
        action="store_true",
        help="Only keep rows whose Zip Code matches the provided list",
    )
    args = parser.parse_args(argv)

    setup_logging()
    if not GOOGLE_API_KEY:
        logging.error("GOOGLE_API_KEY is required")
        raise SystemExit(1)

    smb_restaurants_data.clear()
    if args.zips:
        zip_list = [z.strip() for z in args.zips.split(",") if z.strip()]
    else:
        zip_list = [str(z) for z in TARGET_OLYMPIA_ZIPS]

    for fetcher_cls, enabled in FETCHERS:
        if not enabled:
            continue
        fetcher = fetcher_cls()
        smb_restaurants_data.extend(fetcher.fetch(zip_list))

    if not smb_restaurants_data:
        logging.info("No SMB restaurants found – nothing to write.")
        return

    df = pd.DataFrame(smb_restaurants_data)
    if args.strict_zips and "Zip Code" in df.columns:
        df = df[df["Zip Code"].astype(str).isin(zip_list)]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = f"olympia_smb_google_restaurants_{timestamp}.csv"
    df.to_csv(out_csv, index=False)
    logging.info("Saved %s rows to %s", len(df), out_csv)

    csv_path = pathlib.Path(out_csv)
    loader.load(csv_path)

    conn = sqlite3.connect(loader.DB_PATH)
    df_db = pd.read_sql_query("SELECT * FROM places", conn)
    final_csv = f"olympia_smb_google_restaurants_enriched_{timestamp}.csv"
    df_db.to_csv(final_csv, index=False)
    conn.close()
    logging.info("Saved enriched data to %s", final_csv)


if __name__ == "__main__":
    main()
