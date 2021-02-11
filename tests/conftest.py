"""Test fixtures."""

import os
import threading
from contextlib import contextmanager
from typing import Iterator, Tuple

import pytest
from cheroot import wsgi
from pytest import TempPathFactory
from wsgidav.wsgidav_app import WsgiDAVApp

from webdav4.client import Client
from webdav4.urls import URL

from .utils import TmpDir


@pytest.fixture
def auth() -> Tuple[str, str]:
    """Auth for the server."""
    return "user1", "password1"


@contextmanager
def run_server_on_thread(server: wsgi.Server) -> Iterator[wsgi.Server]:
    """Runs server on a separate thread."""
    server.prepare()
    thread = threading.Thread(target=server.serve)
    thread.daemon = True
    thread.start()

    try:
        yield server
    finally:
        server.stop()
    thread.join()


@pytest.fixture
def storage_dir(tmp_path_factory) -> TmpDir:
    """Storage for webdav server to keep files in."""
    path = os.fspath(tmp_path_factory.mktemp("webdav"))
    return TmpDir(path)


@pytest.fixture
def server(
    tmp_path_factory: TempPathFactory,
    storage_dir: TmpDir,
    auth: Tuple[str, str],
) -> wsgi.Server:
    """Creates a server fixture for testing purpose."""
    host, port = "localhost", 0
    dirmap = {"/": str(storage_dir)}

    user, pwd = auth
    app = WsgiDAVApp(
        {
            "host": host,
            "port": port,
            "provider_mapping": dirmap,
            "simple_dc": {"user_mapping": {"*": {user: {"password": pwd}}}},
        }
    )
    server = wsgi.Server(bind_addr=(host, port), wsgi_app=app)
    with run_server_on_thread(server) as httpd:
        yield httpd


@pytest.fixture
def server_address(server: wsgi.Server) -> URL:
    """Address of the server to contact."""
    return URL("http://{0}:{1}".format("localhost", server.bind_addr[1]))


@pytest.fixture
def client(auth: Tuple[str, str], server_address: URL) -> Client:
    """Webdav client to interact with the server."""
    return Client(server_address, auth=auth)
