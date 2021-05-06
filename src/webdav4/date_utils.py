"""Date parsing utilities."""

from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING

from dateutil.parser import parse

if TYPE_CHECKING:
    from datetime import datetime


def fromisoformat(datetime_string: str) -> "datetime":
    """Convert ISO 8601 datetime string to datetime object."""
    return parse(datetime_string)


def from_rfc1123(datetime_string: str) -> "datetime":
    """Convert rfc1123 datetime string to datetime object."""
    try:
        return parsedate_to_datetime(datetime_string)
    except Exception:  # noqa:E722, pylint: disable=broad-except
        # fallback in case ^ is unable to parse the datetime string
        return parse(datetime_string)
