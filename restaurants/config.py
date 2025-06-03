"""Central configuration for API keys and constants used in the Olympia services project.

Loads API keys from environment variables and defines location-based constants for
Olympia, WA, including ZIP codes and geographic coordinates.
"""

from __future__ import annotations

import os
from typing import Any, List
import logging

# Set up basic logging to warn about configuration issues
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Load environment variables from .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*_a: Any, **_kw: Any) -> None:
        logger.warning("python-dotenv not installed, .env file not loaded")
        return None

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
    "98501", "98502"
]

# Geographic coordinates for Olympia, WA
OLYMPIA_LAT: float = 47.0379
OLYMPIA_LON: float = -122.9007
