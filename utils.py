import re

THIN_SPACE_CHARS = '\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a'


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
