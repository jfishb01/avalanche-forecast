import os
import pytest
from datetime import date

from src.schemas.feature_sets.avalanche_forecast import ForecastDistributorEnum
from src.ingestion.avalanche_forecast.ingestion_helpers import (
    forecast_filename,
    publish_date_from_forecast_filename,
)


def test_forecast_filename():
    # Validate that the filename contains the publish date or else this will break the ingestion pipeline
    actual = forecast_filename(
        ForecastDistributorEnum.CAIC, date(2000, 1, 1), "example"
    )
    assert os.path.basename(actual) == "2000-01-01.json"


def test_publish_date_from_forecast_filename():
    # Validate the publish date can be extracted from a correctly named forecast file
    actual = publish_date_from_forecast_filename("/path/to/file/2000-01-01.json")
    assert actual == date(2000, 1, 1)


def test_publish_date_from_forecast_filename__badly_named_file_raises_exception():
    with pytest.raises(ValueError):
        publish_date_from_forecast_filename("/path/to/file/invalid.json")
