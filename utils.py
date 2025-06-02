import re
import math

THIN_SPACE_CHARS = '\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a'


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great‑circle distance in miles between two lat/lon points."""
    R = 3958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def normalize_hours(hours_dict: dict) -> dict:
    """Return a clean hours dict using en-dash and explicit AM/PM."""
    dash = ' – '
    out = {}

    for day, raw in hours_dict.items():
        if not raw:
            continue
        s = re.sub(f'[{THIN_SPACE_CHARS}]', '', raw)
        if '-' not in s:
            out[day[:3]] = s
            continue
        start, end = [t.strip() for t in s.split('-', 1)]
        ampm = re.search(r'\b(AM|PM)\b', end, re.I)
        if ampm and not re.search(r'\b(AM|PM)\b', start, re.I):
            start = f"{start} {ampm.group(1).upper()}"
        out[day[:3]] = f"{start}{dash}{end.upper()}"
    return out
