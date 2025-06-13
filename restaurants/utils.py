import re
import math
import os
import sys
import logging
import numpy as np
import pandas as pd

THIN_SPACE_CHARS = (
    "\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a"
)

# Basic US ZIP or ZIP+4 format
ZIP_RE = re.compile(r"^\d{5}(?:-\d{4})?$")


def is_valid_zip(zip_code: str) -> bool:
    """Return True if ``zip_code`` is a valid 5-digit or ZIP+4 code."""

    return bool(ZIP_RE.fullmatch(zip_code.strip()))


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float):
    """Great‑circle distance in miles between two lat/lon points.

    Returns ``None`` if any coordinate is ``None`` or NaN.
    """

    for v in (lat1, lon1, lat2, lon2):
        if v is None or math.isnan(v):
            return None

    R = 3958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def haversine_miles_series(
    lat_series: pd.Series,
    lon_series: pd.Series,
    ref_lat: float,
    ref_lon: float,
) -> pd.Series:
    """Vectorized haversine distance in miles to a reference point."""

    lat = lat_series.to_numpy(dtype=float)
    lon = lon_series.to_numpy(dtype=float)

    mask = ~np.isnan(lat) & ~np.isnan(lon)
    result = np.full(lat.shape[0], np.nan)

    if mask.any():
        R = 3958.8
        phi1 = np.radians(lat[mask])
        phi2 = math.radians(ref_lat)
        dphi = np.radians(ref_lat - lat[mask])
        dlambda = np.radians(ref_lon - lon[mask])
        a = (
            np.sin(dphi / 2) ** 2
            + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
        )
        result[mask] = 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    series = pd.Series(result, index=lat_series.index)
    series[~mask] = None
    return series


def normalize_hours(hours_dict: dict) -> dict:
    """Return a clean hours dict using en-dash and explicit AM/PM."""
    dash = " – "
    out = {}

    for day, raw in hours_dict.items():
        if not raw:
            continue
        s = re.sub(f"[{THIN_SPACE_CHARS}]", "", raw)
        if "-" not in s:
            out[day[:3]] = s
            continue
        start, end = [t.strip() for t in s.split("-", 1)]
        ampm = re.search(r"\b(AM|PM)\b", end, re.I)
        if ampm and not re.search(r"\b(AM|PM)\b", start, re.I):
            start = f"{start} {ampm.group(1).upper()}"
        out[day[:3]] = f"{start}{dash}{end.upper()}"
    return out


def setup_logging(level: int = logging.INFO) -> None:
    """Configure logging to stdout or a file.

    If the ``LOG_FILE`` environment variable is set, log messages are written
    to that file. Otherwise, logs are sent to standard output.
    """

    log_file = os.getenv("LOG_FILE")
    handler: logging.Handler
    if log_file:
        handler = logging.FileHandler(log_file)
    else:
        handler = logging.StreamHandler(sys.stdout)

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s: %(message)s",
        handlers=[handler],
    )
