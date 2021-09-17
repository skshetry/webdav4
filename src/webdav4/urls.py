"""URLs parsing logics here."""
from re import sub

from httpx import URL


def strip_leading_slash(path: str) -> str:
    """Strips leading slash from the path, except when it's a root."""
    return path.rstrip("/") if path and path != "/" else path


def normalize_path(path: str) -> str:
    """Normalizes path, removes leading slash."""
    path = sub("/{2,}", "/", path)
    return strip_leading_slash(path)


def join_url(
    base_url: URL, path: str, add_trailing_slash: bool = False
) -> URL:
    """Joins base url with a path."""
    base_path = base_url.path
    path = join_url_path(base_path, path)
    if add_trailing_slash:
        path += "/"
    return base_url.copy_with(path=path)


def join_url_path(hostname: str, path: str) -> str:
    """Returns path absolute."""
    path = path.strip("/")
    return normalize_path(f"/{hostname}/{path}")


def relative_url_to(base_url: URL, rel: str) -> str:
    """Finds relative url to a base url path."""
    base = base_url.path.strip("/")
    rel = rel.strip("/")

    if base == rel or not rel:
        return "/"

    if not base and rel:
        return rel

    index = len(base) + 1
    return rel[index:]
