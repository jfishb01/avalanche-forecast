from datetime import date


def date_to_avalanche_season(d: date) -> str:
    """Get the corresponding avalanche season for the date formatted as <start_year>/<end_year>.

    The avalanche season is defined as running from September 1 through August 31.
    """
    if d.month >= 9:
        return f"{d.year}/{d.year + 1}"
    return f"{d.year - 1}/{d.year}"
