"""fsspec compliant webdav file system."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from fsspec import AbstractFileSystem

from .client import Client
from .file import WebdavFile

if TYPE_CHECKING:
    from ._types import AuthTypes, URLTypes


class WebdavFileSystem(AbstractFileSystem):
    """Provides access to webdav through fsspec-compliant APIs."""

    def __init__(
        self, base_url: "URLTypes", auth: "AuthTypes", client: "Client" = None
    ) -> None:
        """Instantiate WebdavFileSystem with base_url and auth.

        Args:
            base_url: base url of the server
            auth: Authentication to the server
                Refer to HTTPX's auth for more information.
            client: Webdav client to use instead, useful for testing/mocking,
                or extending WebdavFileSystem.
        """
        super().__init__()
        self.client = client or Client(base_url, auth=auth)

    def ls(
        self, path: str, detail: bool = True, **kwargs
    ) -> List[Union[str, Dict[str, Any]]]:
        """`ls` implementation for fsspec, see fsspec for more information."""
        data = self.client.ls(path, detail=detail)
        if not detail:
            return data

        def extract_info(item):
            return {key: item[key] for key in ["name", "size", "type"]}

        return list(map(extract_info, data))

    def rm_file(self, path: str) -> None:
        """Remove a file."""
        return self.client.remove(path)

    def _rm(self, path: str) -> None:
        """Old API for deleting single file, please use `rm_file` instead."""
        return self.rm_file(path)

    def mkdir(self, path: str, create_parents: bool = True, **kwargs) -> None:
        """Create directory."""
        if create_parents:
            return self.makedirs(path, exist_ok=True)
        return self.client.mkdir(path)

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        """Creates directory to the given path."""
        parts = list(filter(bool, path.split("/")))
        paths = ["/".join(parts[: n + 1]) for n in range(len(parts))]
        for parent in paths:
            if not exist_ok and self.isdir(parent):
                raise Exception("oops exists")
            self.mkdir(parent)

    def created(self, path: str) -> str:
        """Returns creation time/date."""
        return self.client.get_property(path, "created") or ""

    def modified(self, path: str) -> str:
        """Returns last modified time/data."""
        return self.client.get_property(path, "modified") or ""

    def mv(
        self, path1, path2, recursive=False, maxdepth=None, **kwargs
    ) -> None:
        """Move a file/directory from one path to the other."""
        return self.client.move(path1, path2)

    def cp_file(self, path1: str, path2: str, **kwargs) -> None:
        """Copy a file/directory from one path to the other."""
        return self.client.copy(path1, path2)

    def open(
        self,
        path: str,
        mode="rb",
        block_size=None,
        cache_options=None,
        **kwargs
    ) -> WebdavFile:
        """Return a file-like object from the filesystem."""
        return WebdavFile(
            self,
            self.client.join(path),
            session=self.client.http,
            block_size=block_size,
            mode=mode,
            size=kwargs.get("size") or self.size(path),
            cache_options=cache_options,
            **kwargs
        )

    def checksum(self, path: str) -> Optional[str]:
        """Returns checksum/etag of the path."""
        return self.client.get_property(path, "etag") or None

    def size(self, path: str) -> Optional[int]:
        """Returns size of the path."""
        size = self.client.get_property(path, "content_length")
        if size is None:
            return None

        assert isinstance(size, int)
        return size

    def sign(self, path: str, expiration: int = 100, **kwargs) -> None:
        """Create a signed URL representing the given path."""
        raise NotImplementedError
