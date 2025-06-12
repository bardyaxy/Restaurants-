from __future__ import annotations

import os
import logging
import pandas as pd

from .base import BaseFetcher


class GpvFetcher(BaseFetcher):
    """Fetch GPV projection data from a CSV file."""

    CSV_ENV = "GPV_CSV_PATH"

    def fetch(self, zip_codes: list[str], **opts) -> list[dict]:
        path = os.getenv(self.CSV_ENV)
        if not path or not os.path.exists(path):
            logging.error("GPV CSV not found; set %s", self.CSV_ENV)
            return []

        df = pd.read_csv(path)
        if "Place ID" not in df.columns:
            logging.error("GPV CSV missing 'Place ID' column")
            return []
        if "GPV Projection" not in df.columns:
            logging.error("GPV CSV missing 'GPV Projection' column")
            return []

        df = df[["Place ID", "GPV Projection"]].copy()
        df["source"] = "gpv_projection"
        return df.to_dict(orient="records")

