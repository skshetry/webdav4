"""Test utilities here."""

from pathlib import Path

PathType = type(Path())


class TmpDir(PathType):
    """Extends Path with `cat` and `gen` methods."""

    def cat(self):
        """Returns (potentially multiple) paths' contents.

        Returns:
            a dict of {path: contents} if the path is a directory,
            otherwise the path contents is returned.
        """
        if self.is_dir():
            return {path.name: path.cat() for path in self.iterdir()}
        return self.read_text()

    def gen(self, struct, text=""):
        """Creates folder structure locally from the provided structure.

        Args:
            root: root of the folder
            struct: the structure to create, can be a dict or a str.
                Dictionary can be nested, which it will create a directory.
                If it's a string, a file with `text` is created.
            text: optional, only necessary if struct is passed a string.
        """
        if isinstance(struct, (str, bytes, Path)):
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
        return struct.keys()
