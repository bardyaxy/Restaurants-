"""Utility functions for network-related checks."""

import logging
import requests


def check_network(
    url: str = "https://www.google.com", timeout: int = 5, method: str = "GET"
) -> bool:
    """Return True if network is reachable.

    A lightweight ``GET`` request is used by default as some networks block ``HEAD``
    requests. You can override ``method`` to ``"HEAD"`` if desired or specify a
    custom URL.
    """

    try:
        if method.upper() == "HEAD":
            requests.head(url, timeout=timeout)
        else:
            # ``allow_redirects`` avoids downloading large responses
            requests.get(url, timeout=timeout, allow_redirects=False)
        return True
    except requests.exceptions.SSLError as exc:
        logging.error("SSL error when checking %s: %s", url, exc)
        return False
    except requests.RequestException:
        return False

