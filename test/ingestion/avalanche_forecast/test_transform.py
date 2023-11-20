import os
import uuid
import json
import glob
import shutil
import pytest
from unittest.mock import patch
from datetime import date, timedelta
from typing import Iterable, List
from fastapi import HTTPException
from pydantic import ValidationError

from src.ingestion.avalanche_forecast import transform
from src.ingestion.avalanche_forecast.common import (
    TransformedAvalancheForecast,
    forecast_filename,
)


@pytest.fixture
def dest():
    dest = f"/tmp/test/{str(uuid.uuid4())}"
    os.makedirs(dest, exist_ok=True)
    yield dest
    shutil.rmtree(dest, ignore_errors=True)


@pytest.fixture
def mock_caic_transformer():
    def transformer(
        start_date: date, end_date: date, src: str
    ) -> Iterable[List[TransformedAvalancheForecast]]:
        num_regions = int(src)
        current_date = start_date
        while current_date <= end_date:
            transformed = []
            for _ in range(num_regions):
                transformed.append(
                    TransformedAvalancheForecast(
                        distributor="CAIC",
                        analysis_date=current_date,
                        forecast_date=current_date,
                        avalanche_season="2000/2001",
                        area_name="dummy",
                        area_id="dummy",
                        polygons="abc,123",
                        avalanche_summary="",
                    )
                )
            yield transformed
            current_date += timedelta(days=1)

    with patch(
        "src.ingestion.avalanche_forecast.distributors.caic.transform",
        side_effect=transformer,
    ) as m:
        yield m


@pytest.mark.parametrize(
    "desc,distributors,start_date,end_date,num_regions_in_raw_file",
    [
        (
            "Transformed files saved for distributors over date range for raw file with single region",
            ["CAIC"],
            date(2000, 1, 1),
            date(2000, 1, 3),
            1,
        ),
        (
            "Transformed files saved for distributors over date range for raw file with multiple regions",
            ["CAIC"],
            date(2000, 1, 1),
            date(2000, 1, 3),
            3,
        ),
    ],
)
def test_transform(
    dest,
    mock_caic_transformer,
    desc,
    distributors,
    start_date,
    end_date,
    num_regions_in_raw_file,
):
    transform.transform(
        transform.ApiQueryParams(
            distributors=distributors,
            start_date=start_date,
            end_date=end_date,
            src=str(num_regions_in_raw_file),
            dest=dest,
        )
    )
    for distributor in distributors:
        current_date = start_date
        while current_date <= end_date:
            with open(forecast_filename(distributor, current_date, dest), "r") as f:
                contents = json.loads(f.read())
                assert len(contents) == num_regions_in_raw_file
            current_date += timedelta(days=1)


@pytest.mark.parametrize(
    "desc,distributors,start_date,end_date",
    [
        (
            "No files saved if no regions in raw file",
            ["CAIC"],
            date(2000, 1, 1),
            date(2000, 1, 3),
        ),
        (
            "No files saved if no distributor provided",
            [],
            date(2000, 1, 1),
            date(2000, 1, 3),
        ),
        (
            "No files saved if start date is greater than end date",
            ["CAIC"],
            date(2000, 1, 3),
            date(2000, 1, 1),
        ),
    ],
)
def test_transform__empty_output(
    dest, mock_caic_transformer, desc, distributors, start_date, end_date
):
    transform.transform(
        transform.ApiQueryParams(
            distributors=distributors,
            start_date=start_date,
            end_date=end_date,
            src="0",
            dest=dest,
        )
    )
    assert len(glob.glob(f"{dest}/*")) == 0


@pytest.mark.parametrize(
    "desc,distributors,start_date,end_date",
    [
        (
            "Error during transformation raises HttpException",
            ["CAIC"],
            date(2000, 1, 1),
            date(2000, 1, 3),
        ),
    ],
)
def test_transform__http_exception_raised(
    dest, desc, distributors, start_date, end_date
):
    def fail_transformation():
        raise RuntimeError

    with patch(
        "src.ingestion.avalanche_forecast.distributors.caic.transform",
        side_effect=fail_transformation,
    ):
        with pytest.raises(HTTPException):
            transform.transform(
                transform.ApiQueryParams(
                    distributors=distributors,
                    start_date=start_date,
                    end_date=end_date,
                    src="dummy",
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
def test_transform__invalid_distributor_raises_exception(
    dest, desc, distributors, start_date, end_date
):
    with pytest.raises(ValidationError):
        transform.transform(
            transform.ApiQueryParams(
                distributors=distributors,
                start_date=start_date,
                end_date=end_date,
                src="dummy",
                dest=dest,
            )
        )
