"""Test fixtures."""

from typing import Tuple

import pytest
from cheroot import wsgi

from webdav4.client import Client
from webdav4.fsspec import WebdavFileSystem
from webdav4.urls import URL

from .server import AUTH, get_server_address, run_server
from .utils import TmpDir


@pytest.fixture(autouse=True)
def reduce_backoff_factor():
    """Reduce backoff factor in tests."""
    from webdav4 import retry

    retry.BACKOFF = 0.001


@pytest.fixture
def auth() -> Tuple[str, str]:
    """Auth for the server."""
    return AUTH


@pytest.fixture
def storage_dir(tmp_path_factory) -> TmpDir:
    """Storage for webdav server to keep files in."""
    path = tmp_path_factory.mktemp("webdav")
    return TmpDir(path)


@pytest.fixture
def server(
    storage_dir: TmpDir,
    auth: Tuple[str, str],
) -> wsgi.Server:
    """Creates a server fixture for testing purpose."""
    with run_server("localhost", 0, str(storage_dir), auth) as (httpd, _):
        yield httpd


@pytest.fixture
def server_address(server: wsgi.Server) -> URL:
    """Address of the server to contact."""
    return get_server_address(server)


@pytest.fixture
def client(auth: Tuple[str, str], server_address: URL) -> Client:
    """Webdav client to interact with the server."""
    return Client(server_address, auth=auth)


@pytest.fixture
def fs(client: Client, server_address: URL) -> WebdavFileSystem:
    """Fixture of WebdavFileSystem to interact with the server."""
    return WebdavFileSystem(server_address, client=client)
