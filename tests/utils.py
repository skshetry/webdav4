"""Test utilities here."""
import os
from pathlib import PosixPath, WindowsPath
from typing import Any, Dict, Iterable, Union, cast

PathClass = PosixPath if os.name == "posix" else WindowsPath


class TmpDir(PathClass):  # type: ignore
    """Extends Path with `cat` and `gen` methods."""

    def cat(self) -> Union[str, Dict[str, Any]]:
        """Returns (potentially multiple) paths' contents.

        Returns:
            a dict of {path: contents} if the path is a directory,
            otherwise the path contents is returned.
        """
        if self.is_dir():
            return {path.name: path.cat() for path in self.iterdir()}
        return cast(str, self.read_text())

    def gen(
        self, struct: Union[str, Dict[str, Any]], text: Union[str, bytes] = ""
    ) -> Iterable[str]:
        """Creates folder structure locally from the provided structure.

        Args:
            root: root of the folder
            struct: the structure to create, can be a dict or a str.
                Dictionary can be nested, which it will create a directory.
                If it's a string, a file with `text` is created.
            text: optional, only necessary if struct is passed a string.
        """
        if isinstance(struct, (str, bytes, PathClass)):
            struct = {struct: text}
        for name, contents in struct.items():
            path = self / name

            if isinstance(contents, dict):
                if not contents:
                    path.mkdir(parents=True, exist_ok=True)
                else:
                    path.gen(contents)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                if isinstance(contents, bytes):
                    path.write_bytes(contents)
                else:
                    path.write_text(contents, encoding="utf-8")

        return list(struct.keys())
