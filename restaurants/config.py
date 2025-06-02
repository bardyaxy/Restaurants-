"""Central configuration for API keys and constants."""

from __future__ import annotations

import os
from typing import Any

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover - fallback when python-dotenv isn't installed
    def load_dotenv(*_a: Any, **_kw: Any) -> None:
        return None

load_dotenv()

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
YELP_API_KEY = os.getenv("YELP_API_KEY")
DOORDASH_API_KEY = os.getenv("DOORDASH_API_KEY")
UBER_EATS_API_KEY = os.getenv("UBER_EATS_API_KEY")

_required = {"GOOGLE_API_KEY": GOOGLE_API_KEY}
_missing = [name for name, val in _required.items() if not val]
if _missing:
    raise SystemExit(
        f"Missing required env vars: {', '.join(_missing)}. "
        "Add them to your .env or export them before running."
    )

TARGET_OLYMPIA_ZIPS = ["98501"]
OLYMPIA_LAT = 47.0379
OLYMPIA_LON = -122.9007
