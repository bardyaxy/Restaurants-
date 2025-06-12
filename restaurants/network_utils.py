"""Utility functions for network-related checks."""

import logging
import os
import requests


def check_network(
    url: str = "https://www.google.com", timeout: int = 5, method: str = "GET"
) -> bool:
    """Return True if network is reachable.

    A lightweight ``GET`` request is used by default since some
    networks block ``HEAD`` requests. Override ``method`` to
    ``"HEAD"`` if desired or specify a custom URL.

    The URL, method, and timeout may also be overridden with environment
    variables ``NETWORK_TEST_URL``, ``NETWORK_TEST_METHOD`` and
    ``NETWORK_TEST_TIMEOUT``. This makes the connectivity check configurable on
    restricted networks.
    """
    url = os.getenv("NETWORK_TEST_URL", url)
    method = os.getenv("NETWORK_TEST_METHOD", method)
    timeout_env = os.getenv("NETWORK_TEST_TIMEOUT")
    if timeout_env:
        try:
            timeout = int(timeout_env)
        except ValueError:
            logging.error(
                "Invalid NETWORK_TEST_TIMEOUT value: %s", timeout_env
            )

    try:
        if method.upper() == "HEAD":
            # ``allow_redirects`` avoids downloading large responses and
            # is explicitly disabled for ``HEAD`` requests as well
            requests.head(url, timeout=timeout, allow_redirects=False)
        else:
            # ``allow_redirects`` avoids downloading large responses
            requests.get(url, timeout=timeout, allow_redirects=False)
        return True
    except requests.exceptions.SSLError as exc:
        logging.error("SSL error when checking %s: %s", url, exc)
        return False
    except requests.RequestException:
        return False
