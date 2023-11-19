import pytest
from datetime import date

from src.utils.datetime_helpers import date_to_avalanche_season


@pytest.mark.parametrize(
    "desc,date_to_test,expected",
    [
        (
            "Date in start year returns current year and next year",
            date(2000, 12, 1),
            "2000/2001",
        ),
        (
            "Date in end year returns prior year and current year",
            date(2000, 1, 1),
            "1999/2000",
        ),
        ("September 1 is the start of the season", date(2000, 9, 1), "2000/2001"),
        ("August 31 is the end of the season", date(2000, 8, 31), "1999/2000"),
    ],
)
def test_date_to_avalanche_season(desc, date_to_test, expected):
    actual = date_to_avalanche_season(date_to_test)
    assert actual == expected
