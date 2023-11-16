import os
import uuid
import shutil
import pytest
from unittest.mock import patch
from datetime import date, timedelta
from typing import Iterable
from fastapi import HTTPException

from src.ingestion.avalanche_forecast import extract
from src.ingestion.avalanche_forecast.custom_types import RawAvalancheForecast
from src.ingestion.avalanche_forecast.distributors import caic


@pytest.fixture
def output_dest():
    dest = f"/tmp/test/{str(uuid.uuid4())}"
    os.makedirs("dest", exist_ok=True)
    yield dest
    shutil.rmtree(dest, ignore_errors=True)


@pytest.fixture
def mock_get_extractor():
    def extractor(start_date: date, end_date: date) -> Iterable[RawAvalancheForecast]:
        current_date = start_date
        while current_date <= end_date:
            yield RawAvalancheForecast(
                analysis_date=current_date, forecast=current_date.isoformat()
            )
            current_date += timedelta(days=1)

    def get_extractor(distributor: str):
        if distributor == "invalid":
            raise RuntimeError
        return extractor

    with patch(
        "src.ingestion.avalanche_forecast.extract.get_extractor",
        side_effect=get_extractor,
    ) as m:
        yield m


@pytest.mark.parametrize(
    "desc,distributors,start_date,end_date",
    [
        (
            "Forecast files saved for distributors over date date range",
            ["Example1", "Example2"],
            date(2000, 1, 1),
            date(2000, 1, 3),
        ),
        (
            "No Forecast files saved if no distributor provided",
            [],
            date(2000, 1, 1),
            date(2000, 1, 3),
        ),
        (
            "No Forecast files saved if start date is greater than end date",
            ["Example1"],
            date(2000, 1, 3),
            date(2000, 1, 1),
        ),
    ],
)
def test_extract(
    output_dest, mock_get_extractor, desc, distributors, start_date, end_date
):
    extract.extract(
        extract.ApiQueryParams(
            distributors=distributors,
            start_date=start_date,
            end_date=end_date,
            dest=output_dest,
        )
    )
    for distributor in distributors:
        output_dir = os.path.join(output_dest, distributor)
        current_date = start_date
        while current_date <= end_date:
            with open(
                os.path.join(output_dir, f"{current_date.isoformat()}.json"), "r"
            ) as f:
                contents = f.read()
                assert contents == current_date.isoformat()
            current_date += timedelta(days=1)


@pytest.mark.parametrize(
    "desc,distributors,start_date,end_date",
    [
        (
            "Error during extraction raises HttpException",
            ["invalid"],
            date(2000, 1, 1),
            date(2000, 1, 3),
        ),
    ],
)
def test_extract__http_exception_raised(
    output_dest, mock_get_extractor, desc, distributors, start_date, end_date
):
    with pytest.raises(HTTPException):
        extract.extract(
            extract.ApiQueryParams(
                distributors=distributors,
                start_date=start_date,
                end_date=end_date,
                dest=output_dest,
            )
        )


@pytest.mark.parametrize(
    "desc,distributor",
    [
        (
            "Valid distributor name returns correct extractor method",
            "CAIC",
        ),
        (
            "Distributor name is case insensitive",
            "caIc",
        ),
    ],
)
def test_get_extractor(desc, distributor):
    assert extract.get_extractor(distributor) == caic.extract


@pytest.mark.parametrize(
    "desc,distributor",
    [
        (
            "Invalid distributor raises KeyError",
            "invalid",
        ),
    ],
)
def test_get_extractor__invalid_distributor_raises_keyerror(desc, distributor):
    with pytest.raises(KeyError):
        extract.get_extractor(distributor)
