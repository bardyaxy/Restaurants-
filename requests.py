class RequestException(Exception):
    pass


class SSLError(RequestException):
    pass


class exceptions:  # simple namespace mimicking requests.exceptions
    RequestException = RequestException
    SSLError = SSLError


def get(*_a, **_kw):
    raise RequestException("network disabled")


def head(*_a, **_kw):
    raise RequestException("network disabled")
