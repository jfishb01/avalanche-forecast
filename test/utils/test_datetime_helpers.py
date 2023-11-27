import pytest
from datetime import date

from src.utils.datetime_helpers import date_to_avalanche_season, date_range


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


@pytest.mark.parametrize(
    "desc,start_date,end_date,inclusive,expected",
    [
        (
            "Date range is generated for multiple dates, end date is included",
            date(2000, 1, 1),
            date(2000, 1, 3),
            True,
            [date(2000, 1, 1), date(2000, 1, 2), date(2000, 1, 3)],
        ),
        (
            "Date range is generated for multiple dates, end date is excluded",
            date(2000, 1, 1),
            date(2000, 1, 3),
            False,
            [date(2000, 1, 1), date(2000, 1, 2)],
        ),
        (
            "Single date generated if start date equals end date",
            date(2000, 1, 1),
            date(2000, 1, 1),
            True,
            [date(2000, 1, 1)],
        ),
        (
            "No dates generated if start date equals end date and end date is excluded",
            date(2000, 1, 1),
            date(2000, 1, 1),
            False,
            [],
        ),
        (
            "No dates generated if start date is greater than end date",
            date(2000, 1, 2),
            date(2000, 1, 1),
            True,
            [],
        ),
        (
            "End date is included by default",
            date(2000, 1, 1),
            date(2000, 1, 2),
            None,
            [date(2000, 1, 1), date(2000, 1, 2)],
        ),
    ],
)
def test_date_range(desc, start_date, end_date, inclusive, expected):
    if inclusive is not None:
        actual = date_range(start_date, end_date, inclusive)
    else:
        actual = date_range(start_date, end_date)
    assert list(actual) == expected
