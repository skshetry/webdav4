"""Provides repl, runs the server on a separate thread.

Tries to replicate pytest fixtures.
Usage: `python -m tests`.
"""

import code
import tempfile
from contextlib import suppress

from webdav4.client import Client

from . import server as _server
from .utils import TmpDir

auth = _server.AUTH
storage_dir = TmpDir(tempfile.mkdtemp("repl"))
with _server.run_server("localhost", 0, str(storage_dir), auth) as server:
    server_address = _server.get_server_address(server)
    client = Client(server_address, auth=auth)

    try:
        from IPython import embed

        embed(colors="neutral")
    except ImportError:
        with suppress(ImportError):
            import readline  # noqa: F401

        shell = code.InteractiveConsole({**globals(), **locals()})
        shell.interact()
