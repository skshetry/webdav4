"""Date parsing utilities."""

from typing import TYPE_CHECKING

from dateutil.parser import parse

if TYPE_CHECKING:
    from datetime import datetime


def fromisoformat(datetime_string: str) -> "datetime":
    """Convert ISO 8601 datetime string to datetime object."""
    return parse(datetime_string)
