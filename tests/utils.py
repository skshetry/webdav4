"""Test utilities here."""
import os
from datetime import datetime, timedelta
from pathlib import PosixPath, WindowsPath
from typing import Any, Dict, Iterable, Union, cast

from _pytest.python_api import ApproxBase

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


class approx_datetime(ApproxBase):
    """Perform approximate comparisons between datetime or timedelta.

    See https://github.com/pytest-dev/pytest/issues/8395#issuecomment-790549327
    """

    default_tolerance = timedelta(seconds=1)
    expected: datetime
    abs: timedelta

    def __init__(
        self,
        expected: datetime,
        abs: timedelta = default_tolerance,
    ) -> None:
        """Initialize the approx_datetime with `abs` as tolerance."""
        assert isinstance(expected, datetime)
        assert abs >= timedelta(
            0
        ), f"absolute tolerance can't be negative: {abs}"
        super().__init__(expected, abs=abs)

    def __repr__(self) -> str:  # pragma: no cover
        """String repr for approx_datetime, shown during failure."""
        return f"approx_datetime({self.expected!r} ± {self.abs!r})"

    def __eq__(self, actual) -> bool:
        """Checking for equality with certain amount of tolerance."""
        if isinstance(actual, datetime):
            return abs(self.expected - actual) <= self.abs
        raise AssertionError(  # pragma: no cover
            "expected type of datetime or timedelta"
        )
