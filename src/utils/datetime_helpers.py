from datetime import date, timedelta
from typing import Iterable


def date_to_avalanche_season(d: date) -> str:
    """Get the corresponding avalanche season for the date formatted as <start_year>/<end_year>.

    The avalanche season is defined as running from September 1 through August 31.
    """
    if d.month >= 9:
        return f"{d.year}/{d.year + 1}"
    return f"{d.year - 1}/{d.year}"


def date_range(
    start_date: date, end_date: date, inclusive: bool = True
) -> Iterable[date]:
    """Generates a list of dates in the range. The end date is included if inclusive=True."""
    current_date = start_date
    if not inclusive:
        end_date -= timedelta(days=1)
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)
