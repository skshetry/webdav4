"""Server for testing purposes, used on repl and pytest as a fixture."""

import threading
from contextlib import contextmanager
from typing import ContextManager, Iterator, Tuple

from cheroot import wsgi
from httpx import URL
from wsgidav.wsgidav_app import WsgiDAVApp

AUTH = "user1", "password1"


def get_server_address(srvr: wsgi.Server) -> URL:
    """Returns base URL of the server."""
    return get_url_from_addr("localhost", srvr.bind_addr[1])


def get_url_from_addr(host, port) -> URL:
    """Builds URL from the host and port."""
    return URL("http://{0}:{1}".format(host, port))


@contextmanager
def run_server_on_thread(srvr: wsgi.Server) -> Iterator[wsgi.Server]:
    """Runs server on a separate thread."""
    srvr.prepare()
    thread = threading.Thread(target=srvr.serve)
    thread.daemon = True
    thread.start()

    try:
        yield srvr
    finally:
        srvr.stop()
    thread.join()


def run_server(
    host: str,
    port: int,
    directory: str,
    authentication: Tuple[str, str],
) -> ContextManager[wsgi.Server]:
    """Runs a webdav server."""
    dirmap = {"/": directory}

    user, pwd = authentication
    app = WsgiDAVApp(
        {
            "host": host,
            "port": port,
            "provider_mapping": dirmap,
            "simple_dc": {"user_mapping": {"*": {user: {"password": pwd}}}},
        }
    )
    return run_server_on_thread(
        wsgi.Server(bind_addr=(host, port), wsgi_app=app)
    )
