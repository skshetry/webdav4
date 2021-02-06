"""HTTP related utilities."""

from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from ._types import HTTPResponse, URLTypes

URL = httpx.URL


def request(method: str):
    """Extending with new verb `method`."""

    def func(client: "Client", url: "URLTypes", **kwargs) -> "HTTPResponse":
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
