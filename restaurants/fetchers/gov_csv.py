import logging
import pandas as pd

from restaurants.network_utils import check_network
from .base import BaseFetcher


class GovCsvFetcher(BaseFetcher):
    """Placeholder fetcher for government CSV imports."""

    def fetch(self, zip_codes: list[str], **opts) -> list[dict]:
        logging.info("Government CSV import disabled in this trimmed script.")
        df = pd.DataFrame(columns=["name", "address", "lat", "lon", "phone", "source", "last_seen"])
        return df.to_dict(orient="records")
