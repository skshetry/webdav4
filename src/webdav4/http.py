"""HTTP related utilities."""

from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from ._types import HTTPResponse, URLTypes

HTTPStatusError = httpx.HTTPStatusError


def request(method: str):  # type: ignore[no-untyped-def]
    """Extending with new verb `method`."""

    def func(
        client: "Client", url: "URLTypes", **kwargs: Any
    ) -> "HTTPResponse":
        return client.request(method, url, **kwargs)

    return func


class Client(httpx.Client):
    """HTTP client with additional verbs for the Webdav."""

    propfind = request("propfind")
    proppatch = request("proppatch")
    mkcol = request("mkcol")
    copy = request("copy")
    move = request("move")
    lock = request("lock")
    unlock = request("unlock")
