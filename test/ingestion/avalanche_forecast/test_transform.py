import os
import uuid
import json
import glob
import shutil
import pytest
from unittest.mock import patch
from datetime import date
from typing import Iterable, List, Callable
from fastapi import HTTPException
from pydantic import ValidationError

from src.utils.datetime_helpers import date_range
from src.ingestion.avalanche_forecast import transform
from src.ingestion.avalanche_forecast.common import (
    ForecastDistributorEnum,
    TransformedAvalancheForecast,
    forecast_filename,
    analysis_date_from_forecast_filename,
)


@pytest.fixture
def src():
    src = f"/tmp/test/{str(uuid.uuid4())}"
    os.makedirs(os.path.join(src, ForecastDistributorEnum.CAIC.name), exist_ok=True)
    yield src
    shutil.rmtree(src, ignore_errors=True)


@pytest.fixture
def dest():
    dest = f"/tmp/test/{str(uuid.uuid4())}"
    os.makedirs(dest, exist_ok=True)
    yield dest
    shutil.rmtree(dest, ignore_errors=True)


@pytest.fixture
def mock_caic_transformer():
    with patch(
        "src.ingestion.avalanche_forecast.distributors.caic.transform",
    ) as m:
        yield m


def mock_transform_fn(
    src: str,
    start_date: date,
    end_date: date,
    num_regions: int,
    create_raw_files: bool = True,
) -> (Callable)[[List[str]], Iterable[List[TransformedAvalancheForecast]]]:
    def transformer(
        filenames: List[str],
    ) -> Iterable[List[TransformedAvalancheForecast]]:
        for filename in filenames:
            transformed = []
            analysis_date = analysis_date_from_forecast_filename(filename)
            for _ in range(num_regions):
                transformed.append(
                    TransformedAvalancheForecast(
                        distributor="CAIC",
                        analysis_date=analysis_date,
                        forecast_date=analysis_date,
                        avalanche_season="2000/2001",
                        area_name="dummy",
                        area_id="dummy",
                        polygons="abc,123",
                        avalanche_summary="",
                    )
                )
            yield transformed

    if create_raw_files:
        for analysis_date in date_range(start_date, end_date):
            with open(
                forecast_filename(ForecastDistributorEnum.CAIC, analysis_date, src), "w"
            ):
                pass

    return transformer


@pytest.mark.parametrize(
    "desc,start_date,end_date,num_regions",
    [
        (
            "Transformed files saved for distributors over date range for raw file with single region",
            date(2000, 1, 1),
            date(2000, 1, 3),
            1,
        ),
        (
            "Transformed files saved for distributors over date range for raw file with multiple regions",
            date(2000, 1, 1),
            date(2000, 1, 3),
            3,
        ),
    ],
)
def test_transform(
    src,
    dest,
    mock_caic_transformer,
    desc,
    start_date,
    end_date,
    num_regions,
):
    distributors = [ForecastDistributorEnum.CAIC]
    mock_caic_transformer.side_effect = mock_transform_fn(
        src, start_date, end_date, num_regions
    )
    transform.transform(
        transform.ApiQueryParams(
            distributors=distributors,
            start_date=start_date,
            end_date=end_date,
            src=src,
            dest=dest,
        )
    )
    for distributor in distributors:
        for analysis_date in date_range(start_date, end_date):
            with open(forecast_filename(distributor, analysis_date, dest), "r") as f:
                contents = json.loads(f.read())
                assert len(contents) == num_regions


@pytest.mark.parametrize(
    "desc,empty_regions,empty_distributors,empty_raw_files,invert_dates",
    [
        ("No files saved if no regions in raw file", True, False, False, False),
        ("No files saved if no distributor provided", False, True, False, False),
        ("No files saved if no raw files exist", False, False, True, False),
        (
            "No files saved if start date is greater than end date",
            False,
            False,
            False,
            True,
        ),
    ],
)
def test_transform__empty_output(
    src,
    dest,
    mock_caic_transformer,
    desc,
    empty_regions,
    empty_distributors,
    empty_raw_files,
    invert_dates,
):
    start_date = date(2000, 1, 1)
    end_date = date(2000, 1, 3)
    num_regions = 0 if empty_regions else 1
    distributors = [] if empty_distributors else [ForecastDistributorEnum.CAIC]
    mock_caic_transformer.side_effect = mock_transform_fn(
        src, start_date, end_date, num_regions, not empty_raw_files
    )
    if invert_dates:
        start_date, end_date = (end_date, start_date)

    transform.transform(
        transform.ApiQueryParams(
            distributors=distributors,
            start_date=start_date,
            end_date=end_date,
            src=src,
            dest=dest,
        )
    )
    assert len(glob.glob(f"{dest}/*")) == 0


def test_transform__transformation_error_raises_http_exception(dest):
    def fail_transformation():
        raise RuntimeError

    with patch(
        "src.ingestion.avalanche_forecast.distributors.caic.transform",
        side_effect=fail_transformation,
    ):
        with pytest.raises(HTTPException):
            transform.transform(
                transform.ApiQueryParams(
                    distributors=[ForecastDistributorEnum.CAIC],
                    start_date=date(2000, 1, 1),
                    end_date=date(2000, 1, 3),
                    src="dummy",
                    dest=dest,
                )
            )


def test_transform__invalid_distributor_raises_exception(dest):
    with pytest.raises(ValidationError):
        transform.transform(
            transform.ApiQueryParams(
                distributors=["invalid"],
                start_date=date(2000, 1, 1),
                end_date=date(2000, 1, 3),
                src="dummy",
                dest=dest,
            )
        )
