"""A File abstraction that could be used in `fs.open()` calls."""

from typing import TYPE_CHECKING

from fsspec.spec import AbstractBufferedFile

if TYPE_CHECKING:
    from .fs import WebdavFileSystem


class WebdavFile(AbstractBufferedFile):
    """WebdavFile that provides file-like access to remote file."""

    def __init__(self, fs: "WebdavFileSystem", path, mode="rb", **kwargs):
        """Instantiate a file-like object with the provided options.

        See fsspec for more information.
        """
        if mode != "rb":
            raise NotImplementedError("File mode not supported")
        super().__init__(fs, path, mode=mode, **kwargs)
        self.fs = fs  # make mypy happy

    def _fetch_range(self, start, end):
        """Fetches a chunk of data between range of start-end."""
        headers = {"range": f"bytes={start}-{end}"}
        url = self.fs.client.base_url.join(self.path)
        return self.fs.client.http.get(url, headers=headers)

    def commit(self) -> None:
        """Move from temp to final destination."""
        return self.fs.mv(self.location, self.path)

    def discard(self) -> None:
        """Discard temp. file."""
        return self.fs.rm_file(self.path)
