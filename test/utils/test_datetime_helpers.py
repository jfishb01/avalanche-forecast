import pytest
from datetime import date

from src.utils.datetime_helpers import (
    date_to_avalanche_season,
    date_range,
    date_in_avalanche_season,
    date_to_day_number_of_avalanche_season,
    get_avalanche_season_date_bounds,
)


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
        ("May 31 is the end of the season", date(2000, 5, 31), "1999/2000"),
        ("June 1 is the start of the off season", date(2000, 6, 1), "OFF_SEASON"),
        ("August 31 is the end of the off season", date(2000, 8, 31), "OFF_SEASON"),
    ],
)
def test_date_to_avalanche_season(desc, date_to_test, expected):
    actual = date_to_avalanche_season(date_to_test)
    assert actual == expected


@pytest.mark.parametrize(
    "desc,date_to_test,expected",
    [
        (
            "Current season is used when in season before Jan 1",
            date(2000, 11, 1),
            "2000/2001",
        ),
        (
            "Current season is used when in season after Jan 1",
            date(2000, 2, 1),
            "1999/2000",
        ),
        ("Upcoming season is used when out of season", date(2000, 7, 1), "2000/2001"),
    ],
)
def test_date_to_avalanche_season__use_upcoming_when_unmapped(
    desc, date_to_test, expected
):
    actual = date_to_avalanche_season(date_to_test, use_upcoming_when_unmapped=True)
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


@pytest.mark.parametrize(
    "desc,date_to_test,expected",
    [
        ("Date in avalanche season returns True", date(2000, 1, 1), True),
        ("Date outside of avalanche season returns False", date(2000, 7, 1), False),
    ],
)
def test_date_in_avalanche_season(desc, date_to_test, expected):
    actual = date_in_avalanche_season(date_to_test)
    assert actual == expected


@pytest.mark.parametrize(
    "desc,date_to_test,expected",
    [
        (
            "Date in avalanche season before the start of the year",
            date(2000, 12, 1),
            91,
        ),
        ("Date in avalanche season after the start of the year", date(2000, 2, 1), 153),
        ("Day numbering is 0 indexed", date(2000, 9, 1), 0),
        ("Date outside of avalanche season", date(2000, 7, 1), -1),
    ],
)
def test_date_to_day_number_of_avalanche_season(desc, date_to_test, expected):
    actual = date_to_day_number_of_avalanche_season(date_to_test)
    assert actual == expected


def test_get_avalanche_season_date_bounds():
    actual = get_avalanche_season_date_bounds("2000/2001")
    assert actual == (date(2000, 9, 1), date(2001, 6, 1))


def test_get_avalanche_season_date_bounds__invalid_season_name():
    with pytest.raises(ValueError):
        get_avalanche_season_date_bounds("invalid")
