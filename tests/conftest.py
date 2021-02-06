"""Test fixtures."""

import os
import threading
from contextlib import contextmanager

import pytest
from cheroot import wsgi
from httpx import URL
from wsgidav.wsgidav_app import WsgiDAVApp

from webdav4.http import Client as HTTPClient

from .utils import TmpDir


@pytest.fixture
def auth():
    """Auth for the server."""
    return "user1", "password1"


@contextmanager
def run_server_on_thread(server: "wsgi.Server"):
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
def storage_dir(tmp_path_factory):
    """Storage for webdav server to keep files in."""
    path = os.fspath(tmp_path_factory.mktemp("webdav"))
    yield TmpDir(path)


@pytest.fixture
def server(tmp_path_factory, storage_dir, auth):
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
def server_address(server):
    """Address of the server to contact."""
    return URL("http://{0}:{1}".format("localhost", server.bind_addr[1]))


@pytest.fixture
def http_client(auth, server):
    """Http client to interact with the server."""
    client = HTTPClient(auth=auth)
    yield client
