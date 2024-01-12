import os
from datetime import date, datetime

from src.schemas.feature_sets.avalanche_forecast import ForecastDistributorEnum


def forecast_filename(
    distributor: ForecastDistributorEnum, publish_date: date, base_dir: str
) -> str:
    """Ensure consistent filenames for avalanche forecast data for both generation and retrieval.

    Without building out a metadata database to track ingested files, we need a way to easily identify
    the publish date that a file refers to. This method creates a clear naming scheme to track records across
    distributors and publish dates. In the long-term, this should be replaced with a database storing metadate
    about the ingested files.
    """
    return os.path.join(
        os.path.join(base_dir, distributor), f"{publish_date.isoformat()}.json"
    )


def publish_date_from_forecast_filename(filename: str) -> date:
    """Get the file published date from a forecast filename.

    In the long-term, this should be replaced with a database that stores this metadate as it is more
    resilient to changes and will allow for improved flexibility.
    """
    return datetime.strptime(os.path.basename(filename), "%Y-%m-%d.json").date()
