from datetime import datetime, date, timedelta
from typing import Iterable, Tuple, Sequence


OFF_SEASON = "OFF_SEASON"
SEASON_START_MONTH = 9
SEASON_START_DAY = 1
SEASON_END_MONTH = 6
SEASON_END_DAY = 1


def date_to_avalanche_season(d: date, use_upcoming_when_unmapped: bool = False) -> str:
    """Get the corresponding avalanche season for the date formatted as <start_year>/<end_year>.

    The avalanche season is defined as running from September 1 through May 31. Anything else is marked as
    "OFF_SEASON".

    Args:
        d: The date to map to an avalanche season.
        use_upcoming_when_unmapped: When False, "OFF_SEASON" will be returned if the date does not map to a
        season. Otherwise, the next upcoming season will be returned. Defaults to False.

    Returns: The corresponding avalanche season as <start_year>/<end_year> or alternatively "OFF_SEASON".
    """
    if not date_in_avalanche_season(d):
        return f"{d.year}/{d.year + 1}" if use_upcoming_when_unmapped else OFF_SEASON

    if d.month >= SEASON_START_MONTH and d.day >= SEASON_START_DAY:
        return f"{d.year}/{d.year + 1}"
    return f"{d.year - 1}/{d.year}"


def date_in_avalanche_season(d: date) -> bool:
    """Return whether the date is in the date bounds of any avalanche season."""
    return d >= date(d.year, SEASON_START_MONTH, SEASON_START_DAY) or d < date(
        d.year, SEASON_END_MONTH, SEASON_END_DAY
    )


def date_to_day_number_of_avalanche_season(d: date) -> int:
    """Get the number of days into the avalanche season for the corresponding date.

    The avalanche season is defined as running from September 1 through August 31, so inputting the date
    "2020-09-05" would yield 5 as September 5, is the fifth day of the 2020/2021 avalanche season.
    """
    if not date_in_avalanche_season(d):
        return -1

    if d.month >= SEASON_START_MONTH and d.day >= SEASON_START_DAY:
        return (d - date(d.year, SEASON_START_MONTH, SEASON_START_DAY)).days
    return (d - date(d.year - 1, SEASON_START_MONTH, SEASON_START_DAY)).days


def get_avalanche_season_date_bounds(
    avalanche_season: str, inclusive: str = "left"
) -> Tuple[date, date]:
    """Get the date bounds of the provided avalanche season."""
    start_year, end_year = avalanche_season.split("/")
    date_bounds = (
        date(int(start_year), SEASON_START_MONTH, SEASON_START_DAY),
        date(int(end_year), SEASON_END_MONTH, SEASON_END_DAY),
    )
    if inclusive.upper() == "LEFT":
        return date_bounds[0], date_bounds[1] - timedelta(days=1)
    if inclusive.upper() == "RIGHT":
        return date_bounds[0] + timedelta(days=1), date_bounds[1]
    if inclusive.upper() == "BOTH":
        return date_bounds
    else:
        raise ValueError(
            "Unhandled value for parameter 'inclusive. Must be one of 'left'|'right'|'both'."
        )


def date_range(
    start_date: date, end_date: date, inclusive: bool = True
) -> Iterable[date]:
    """Generate a list of dates in the range. The end date is included if inclusive=True."""
    current_date = start_date
    if not inclusive:
        end_date -= timedelta(days=1)
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)


def try_strptime(date_string: str, formats: Sequence[str]) -> datetime:
    """Tries to parse date_string into a datetime using formats, returning on the first format to match."""
    for fmt in formats:
        try:
            return datetime.strptime(date_string, fmt)
        except:
            pass
    raise ValueError(
        f"time data '{date_string}' does not match any of the formats: {formats}"
    )
