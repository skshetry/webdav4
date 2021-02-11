"""HTTP related utilities."""

from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from .types import HTTPResponse, URLTypes

HTTPStatusError = httpx.HTTPStatusError


class Method:
    """HTTP methods, trying to prevent mistakes with this."""

    PROPFIND = "PROPFIND"
    PROPPATCH = "PROPPATCH"
    MKCOL = "MKCOL"
    COPY = "COPY"
    MOVE = "MOVE"
    LOCK = "LOCK"
    UNLOCK = "UNLOCK"
    DELETE = "DELETE"
    GET = "GET"
    PUT = "PUT"


def request(method: str):  # type: ignore[no-untyped-def]
    """Extending with new verb `method`."""

    def func(
        client: "Client", url: "URLTypes", **kwargs: Any
    ) -> "HTTPResponse":
        return client.request(method, url, **kwargs)

    return func


class Client(httpx.Client):
    """HTTP client with additional verbs for the Webdav."""

    propfind = request(Method.PROPFIND)
    proppatch = request(Method.PROPPATCH)
    mkcol = request(Method.MKCOL)
    copy = request(Method.COPY)
    move = request(Method.MOVE)
    lock = request(Method.LOCK)
    unlock = request(Method.UNLOCK)
