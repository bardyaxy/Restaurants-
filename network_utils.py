"""Utility functions for network-related checks."""

import requests


def check_network(url: str = "https://www.google.com", timeout: int = 5) -> bool:
    """Return True if network is reachable."""
    try:
        requests.head(url, timeout=timeout)
        return True
    except requests.RequestException:
        return False

