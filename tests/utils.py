"""Test utilities here."""
from datetime import datetime, timedelta

from _pytest.python_api import ApproxBase


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
