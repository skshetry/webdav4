"""Tests for webdav client."""

import pytest
from httpx import URL

from webdav4.http import Client

from .utils import TmpDir


@pytest.mark.parametrize(
    "structure, path, success",
    [
        ({"data": {"foo": "foo", "bar": "bar"}}, "/data", True),
        ({"data": {"foo": "foo", "bar": "bar"}}, "/data/foo", True),
        ({"data": {"bar": "bar"}}, "/data/foo", False),
        ({"data": {"bar": "bar"}}, "/not-existing", False),
    ],
)
def test_client_propfind(
    structure,
    path,
    success,
    storage_dir: "TmpDir",
    http_client: "Client",
    server_address: "URL",
):
    """Test http client's propfind response."""
    storage_dir.gen(structure)
    resp = http_client.propfind(server_address.join(path))
    assert resp.is_error != success
