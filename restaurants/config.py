"""Central configuration for API keys and constants used in the Olympia services project.

Loads API keys from environment variables and defines location-based constants for
Olympia, WA, including ZIP codes and geographic coordinates.
"""

from __future__ import annotations

import os
from typing import Any, List, IO
import logging

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

# List of ZIP codes for the Olympia, WA area
TARGET_OLYMPIA_ZIPS: List[str] = [
    "98330",
    "98360",
    "98492",
    "98496",
    "98444",
    "98303",
    "98498",
    "98499",
    "98388",
    "98497",
    "98351",
    "98584",
    "98531",
    "98512",
    "98530",
    "98556",
    "98579",
    "98557",
    "98511",
    "98507",
    "98504",
    "98599",
    "98505",
    "98508",
    "98502",
    "98506",
    "98589",
    "98576",
    "98558",
    "98540",
    "98501",
    "98513",
    "98503",
    "98509",
    "98327",
    "98433",
    "98431",
    "98516",
    "98430",
    "98439",
    "98328",
    "98580",
    "98438",
    "98338",
    "98344",
    "98387",
    "98445",
    "98446",
    "98447",
    "98448",
    "98355",
    "98533",
    "98597",
    "98348",
    "98544",
    "98522",
    "98532",
]

# Geographic coordinates for Olympia, WA
OLYMPIA_LAT: float = 47.0379
OLYMPIA_LON: float = -122.9007
