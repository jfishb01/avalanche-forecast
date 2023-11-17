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

from src.ingestion.avalanche_forecast import extract
from src.ingestion.avalanche_forecast.common import (
    RawAvalancheForecast,
    forecast_filename,
)


@pytest.fixture
def dest():
    dest = f"/tmp/test/{str(uuid.uuid4())}"
    os.makedirs(dest, exist_ok=True)
    yield dest
    shutil.rmtree(dest, ignore_errors=True)


@pytest.fixture
def mock_caic_extractor():
    def extractor(start_date: date, end_date: date) -> Iterable[RawAvalancheForecast]:
        current_date = start_date
        while current_date <= end_date:
            yield RawAvalancheForecast(
                analysis_date=current_date, forecast=current_date.isoformat()
            )
            current_date += timedelta(days=1)

    with patch(
        "src.ingestion.avalanche_forecast.distributors.caic.extract",
        side_effect=extractor,
    ) as m:
        yield m


@pytest.mark.parametrize(
    "desc,distributors,start_date,end_date",
    [
        (
            "Forecast files saved for distributors over date date range",
            ["CAIC"],
            date(2000, 1, 1),
            date(2000, 1, 3),
        ),
    ],
)
def test_extract(dest, mock_caic_extractor, desc, distributors, start_date, end_date):
    extract.extract(
        extract.ApiQueryParams(
            distributors=distributors,
            start_date=start_date,
            end_date=end_date,
            dest=dest,
        )
    )
    for distributor in distributors:
        current_date = start_date
        while current_date <= end_date:
            with open(forecast_filename(distributor, current_date, dest), "r") as f:
                contents = f.read()
                assert contents == current_date.isoformat()
            current_date += timedelta(days=1)


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


@pytest.mark.parametrize(
    "desc,distributors,start_date,end_date",
    [
        (
            "Error during extraction raises HttpException",
            ["CAIC"],
            date(2000, 1, 1),
            date(2000, 1, 3),
        ),
    ],
)
def test_extract__http_exception_raised(dest, desc, distributors, start_date, end_date):
    def fail_extraction():
        raise RuntimeError

    with patch(
        "src.ingestion.avalanche_forecast.distributors.caic.extract",
        side_effect=fail_extraction,
    ):
        with pytest.raises(HTTPException):
            extract.extract(
                extract.ApiQueryParams(
                    distributors=distributors,
                    start_date=start_date,
                    end_date=end_date,
                    dest=dest,
                )
            )


@pytest.mark.parametrize(
    "desc,distributors,start_date,end_date",
    [
        (
            "Invalid distributor raises ValidationError",
            ["invalid"],
            date(2000, 1, 1),
            date(2000, 1, 3),
        ),
    ],
)
def test_extract__invalid_distributor_raises_exception(
    dest, desc, distributors, start_date, end_date
):
    with pytest.raises(ValidationError):
        extract.extract(
            extract.ApiQueryParams(
                distributors=distributors,
                start_date=start_date,
                end_date=end_date,
                dest=dest,
            )
        )
