"""Configuration for API keys and constants in the Olympia services project.

Loads API keys from environment variables and defines location-based constants
for Olympia, WA, including ZIP codes and geographic coordinates.
"""

from __future__ import annotations

import logging
import os
import pathlib
from typing import IO, List

from restaurants.utils import is_valid_zip

# Set up basic logging to warn about configuration issues
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Load environment variables from .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
except ImportError:
    from os import PathLike

    def load_dotenv(
        dotenv_path: str | PathLike[str] | None = None,
        stream: IO[str] | None = None,
        verbose: bool = False,
        override: bool = False,
        interpolate: bool = True,
        encoding: str | None = "utf-8",
    ) -> bool:
        logger.warning("python-dotenv not installed, .env file not loaded")
        return False


load_dotenv()

# API keys from environment variables
GOOGLE_API_KEY: str | None = os.getenv("GOOGLE_API_KEY")
YELP_API_KEY: str | None = os.getenv("YELP_API_KEY")
DOORDASH_API_KEY: str | None = os.getenv("DOORDASH_API_KEY")
UBER_EATS_API_KEY: str | None = os.getenv("UBER_EATS_API_KEY")

# Default ZIP code for Olympia, WA
DEFAULT_ZIP: str = "98501"

# Check for required environment variables
_required = {
    "GOOGLE_API_KEY": GOOGLE_API_KEY,
    "YELP_API_KEY": YELP_API_KEY,
}
_missing = [name for name, val in _required.items() if not val]
if _missing:
    logger.warning(
        f"Missing required env vars: {', '.join(_missing)}. "
        "Add them to your .env file or export them before running."
    )

# ---------------------------------------------------------------------------
# ZIP code loading
# ---------------------------------------------------------------------------
ZIP_FILE = pathlib.Path(__file__).resolve().parents[1] / "toast_zips.txt"


def load_zip_codes(path: pathlib.Path = ZIP_FILE) -> list[str]:
    """Return a list of ZIP codes from ``path`` ignoring invalid lines."""
    try:
        with path.open("r", encoding="utf-8") as f:
            codes: list[str] = []
            for line in f:
                code = line.strip()
                if not code:
                    continue
                if is_valid_zip(code):
                    codes.append(code)
                else:
                    logger.warning("Invalid ZIP code ignored: %s", code)
            return codes
    except FileNotFoundError:
        logger.warning("ZIP code file not found: %s", path)
        return []


# List of ZIP codes for the Olympia, WA area loaded at import time
TARGET_OLYMPIA_ZIPS: List[str] = load_zip_codes()

# Geographic coordinates for Olympia, WA
OLYMPIA_LAT: float = 47.0379
OLYMPIA_LON: float = -122.9007
