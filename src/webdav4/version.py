"""Version of the library."""

try:
    from ._version import version as __version__
except ImportError:  # pragma: no cover
    __version__ = "UNKNOWN"
