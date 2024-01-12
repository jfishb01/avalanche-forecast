import os
import uuid
import glob
import shutil
import pytest
from unittest.mock import patch
from datetime import date, timedelta
from typing import Iterable
from fastapi import HTTPException
from pydantic import ValidationError

from src.utils.datetime_helpers import date_range
from src.ingestion.avalanche_forecast import extract
from src.ingestion.avalanche_forecast.ingestion_helpers import forecast_filename
from src.schemas.feature_sets.avalanche_forecast import RawAvalancheForecast


@pytest.fixture
def dest():
    dest = f"/tmp/test/{str(uuid.uuid4())}"
    os.makedirs(dest, exist_ok=True)
    yield dest
    shutil.rmtree(dest, ignore_errors=True)


@pytest.fixture
def mock_caic_extractor():
    def extractor(start_date: date, end_date: date) -> Iterable[RawAvalancheForecast]:
        for publish_date in date_range(start_date, end_date):
            yield RawAvalancheForecast(
                publish_date=publish_date, forecast=publish_date.isoformat()
            )
            publish_date += timedelta(days=1)

    with patch(
        "src.ingestion.avalanche_forecast.distributors.caic.extract",
        side_effect=extractor,
    ) as m:
        yield m


def test_extract(dest, mock_caic_extractor):
    distributors = ["CAIC"]
    start_date = date(2000, 1, 1)
    end_date = date(2000, 1, 3)
    extract.extract(
        extract.ApiQueryParams(
            distributors=distributors,
            start_date=start_date,
            end_date=end_date,
            dest=dest,
        )
    )
    for distributor in distributors:
        for publish_date in date_range(start_date, end_date):
            with open(forecast_filename(distributor, publish_date, dest), "r") as f:
                contents = f.read()
                assert contents == publish_date.isoformat()


@pytest.mark.parametrize(
    "desc,distributors,start_date,end_date",
    [
        (
            "No forecast files saved if no distributor provided",
            [],
            date(2000, 1, 1),
            date(2000, 1, 3),
        ),
        (
            "No forecast files saved if start date is greater than end date",
            ["CAIC"],
            date(2000, 1, 3),
            date(2000, 1, 1),
        ),
    ],
)
def test_extract__empty_output(
    dest, mock_caic_extractor, desc, distributors, start_date, end_date
):
    extract.extract(
        extract.ApiQueryParams(
            distributors=distributors,
            start_date=start_date,
            end_date=end_date,
            dest=dest,
        )
    )
    assert len(glob.glob(f"{dest}/*")) == 0


def test_extract__extraction_error_raises_http_exception(dest):
    def fail_extraction():
        raise RuntimeError

    with patch(
        "src.ingestion.avalanche_forecast.distributors.caic.extract",
        side_effect=fail_extraction,
    ):
        with pytest.raises(HTTPException):
            extract.extract(
                extract.ApiQueryParams(
                    distributors=["CAIC"],
                    start_date=date(2000, 1, 1),
                    end_date=date(2000, 1, 3),
                    dest=dest,
                )
            )


def test_extract__invalid_distributor_raises_exception(dest):
    with pytest.raises(ValidationError):
        extract.extract(
            extract.ApiQueryParams(
                distributors=["invalid"],
                start_date=date(2000, 1, 1),
                end_date=date(2000, 1, 3),
                dest=dest,
            )
        )
