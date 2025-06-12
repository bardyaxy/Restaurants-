import logging
import pandas as pd

from restaurants.network_utils import check_network
from .base import BaseFetcher


class OsmFetcher(BaseFetcher):
    """Placeholder fetcher for OpenStreetMap."""

    def fetch(self, zip_codes: list[str], **opts) -> list[dict]:
        if not check_network():
            logging.error("Network unavailable; cannot fetch OSM data.")
            raise SystemExit(1)
        # … (same as before) – omitted here for brevity
        return pd.DataFrame().to_dict(orient="records")
