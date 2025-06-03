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

DEFAULT_ZIP = "98501"

_required = {"GOOGLE_API_KEY": GOOGLE_API_KEY}
_missing = [name for name, val in _required.items() if not val]
if _missing:
    raise SystemExit(
        f"Missing required env vars: {', '.join(_missing)}. "
        "Add them to your .env or export them before running."
    )

TARGET_OLYMPIA_ZIPS = [
    98303,
    98327,
    98328,
    98330,
    98338,
    98344,
    98348,
    98351,
    98355,
    98360,
    98387,
    98388,
    98430,
    98431,
    98433,
    98438,
    98439,
    98444,
    98445,
    98446,
    98447,
    98448,
    98492,
    98496,
    98497,
    98498,
    98499,
    98501,
    98502,
    98503,
    98504,
    98505,
    98506,
    98507,
    98508,
    98509,
    98511,
    98512,
    98513,
    98516,
    98522,
    98530,
    98531,
    98532,
    98533,
    98540,
    98544,
    98556,
    98557,
    98558,
    98576,
    98579,
    98580,
    98584,
    98589,
    98597,
    98599,
]
OLYMPIA_LAT = 47.0379
OLYMPIA_LON = -122.9007
