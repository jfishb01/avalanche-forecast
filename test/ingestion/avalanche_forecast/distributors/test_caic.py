import os
import json
import uuid
import pytest
import shutil
from datetime import date, timedelta
from unittest.mock import patch
from requests import HTTPError
from typing import Iterable, List, Dict, Any

from src.ingestion.avalanche_forecast.distributors.caic import extract, transform
from src.ingestion.avalanche_forecast.common import (
    forecast_filename,
    RawAvalancheForecast,
    TransformedAvalancheForecast,
    ForecastDistributorEnum,
    AvalancheRiskEnum,
    AvalancheProblemEnum,
    AvalancheLikelihoodEnum,
)


def get_sample_raw_data(region_ids: Iterable[str]) -> List[Dict[str, Any]]:
    raw_data = []
    for id in region_ids:
        raw_data.append(
            {
                "title": f"title_{id}",
                "type": "avalancheforecast",
                "polygons": [f"polygon0_{id}", f"polygon1_{id}"],
                "areaId": f"area_{id}",
                "avalancheSummary": {"days": [{"content": f"summary_{id}"}]},
                "dangerRatings": {
                    "days": [{"alp": "considerable", "tln": "moderate", "btl": "low"}]
                },
                "avalancheProblems": {
                    "days": [
                        [
                            {
                                "type": "persistentSlab",
                                "aspectElevations": ["n_alp"],
                                "likelihood": "possible",
                                "expectedSize": {"min": "1.0", "max": "1.5"},
                            }
                        ]
                    ]
                },
            }
        )
    return raw_data


def save_sample_raw_data(
    raw_data: List[Dict[str, Any]], dir: str, start_date: date, end_date: date
) -> None:
    current_date = start_date
    while current_date <= end_date:
        raw_data_filename = forecast_filename(
            ForecastDistributorEnum.CAIC, current_date, dir
        )
        os.makedirs(os.path.dirname(raw_data_filename), exist_ok=True)
        with open(raw_data_filename, "w") as f:
            f.write(json.dumps(raw_data))
        current_date += timedelta(days=1)


@pytest.fixture
def src():
    src = f"/tmp/test/{str(uuid.uuid4())}"
    os.makedirs(src, exist_ok=True)
    yield src
    shutil.rmtree(src, ignore_errors=True)


@pytest.fixture
def mock_response():
    class Response:
        text = "Lorem Ipsum"

        def __init__(self, raise_exception=False):
            self._raise_exception = raise_exception

        def raise_for_status(self):
            if self._raise_exception:
                raise HTTPError

    def generate_response(url):
        if "invalid" in url:
            return Response(raise_exception=True)
        return Response()

    with patch("requests.get", side_effect=generate_response) as m:
        yield m


@pytest.mark.parametrize(
    "desc,start_date,end_date",
    [
        ("Results extracted for each date", date(2000, 1, 1), date(2000, 1, 3)),
        ("No results if start date > end date", date(2000, 1, 3), date(2000, 1, 1)),
    ],
)
def test_extract(mock_response, desc, start_date, end_date):
    expected = []
    current_date = start_date
    while current_date <= end_date:
        expected.append(
            RawAvalancheForecast(analysis_date=current_date, forecast="Lorem Ipsum")
        )
        current_date += timedelta(days=1)
    with patch(
        "src.ingestion.avalanche_forecast.distributors.caic._get_url",
        return_value="https://example.com",
    ):
        actual = list(extract(start_date, end_date))
    assert actual == expected


@pytest.mark.parametrize(
    "desc,start_date,end_date",
    [
        ("Bad request raises HTTPError", date(2000, 1, 1), date(2000, 1, 3)),
    ],
)
def test_extract__bad_request_raises_error(mock_response, desc, start_date, end_date):
    with patch(
        "src.ingestion.avalanche_forecast.distributors.caic._get_url",
        return_value="invalid",
    ):
        with pytest.raises(HTTPError):
            list(extract(start_date, end_date))


@pytest.mark.parametrize(
    "desc,start_date,end_date,region_ids,raw_data_fields_to_update,expected_fields_to_update",
    [
        (
            "Data transformed with single region",
            date(2000, 1, 1),
            date(2000, 1, 1),
            ["0"],
            {},
            {},
        ),
        (
            "Data transformed with multiple regions",
            date(2000, 1, 1),
            date(2000, 1, 1),
            ["0", "1"],
            {},
            {},
        ),
        (
            "Data transformed with single region over multiple days",
            date(2000, 1, 1),
            date(2000, 1, 3),
            ["0"],
            {},
            {},
        ),
        (
            "Data transformed with multiple regions over multiple days",
            date(2000, 1, 1),
            date(2000, 1, 3),
            ["0", "1"],
            {},
            {},
        ),
        (
            "Unused fields have no effect",
            date(2000, 1, 1),
            date(2000, 1, 1),
            ["0"],
            {"unused": "val"},
            {},
        ),
        (
            "Empty summary used if not provided",
            date(2000, 1, 1),
            date(2000, 1, 1),
            ["0"],
            {"avalancheSummary": {"days": []}},
            {"avalanche_summary": ""},
        ),
        (
            "No problems still works",
            date(2000, 1, 1),
            date(2000, 1, 1),
            ["0"],
            {"avalancheProblems": {"days": [[]]}},
            {
                "problem_0": AvalancheProblemEnum.NOFORECAST,
                "likelihood_0": AvalancheLikelihoodEnum.NOFORECAST,
                "min_size_0": -1.0,
                "max_size_0": -1.0,
                "n_alp_0": False,
            },
        ),
    ],
)
def test_transform(
    src,
    desc,
    start_date,
    end_date,
    region_ids,
    raw_data_fields_to_update,
    expected_fields_to_update,
):
    raw_data = get_sample_raw_data(region_ids)
    for entry in raw_data:
        entry |= raw_data_fields_to_update
    save_sample_raw_data(raw_data, src, start_date, end_date)

    current_date = start_date
    expected = []
    while current_date <= end_date:
        expected_on_date = []
        for id in region_ids:
            transformed = TransformedAvalancheForecast(
                distributor=ForecastDistributorEnum.CAIC,
                analysis_date=current_date,
                forecast_date=current_date,
                avalanche_season="1999/2000",
                area_name=f"title_{id}",
                area_id=f"area_{id}",
                polygons=f"polygon0_{id},polygon1_{id}",
                avalanche_summary=f"summary_{id}",
                danger_alp=AvalancheRiskEnum.CONSIDERABLE,
                danger_tln=AvalancheRiskEnum.MODERATE,
                danger_btl=AvalancheRiskEnum.LOW,
                problem_0=AvalancheProblemEnum.PERSISTENTSLAB,
                likelihood_0=AvalancheLikelihoodEnum.POSSIBLE,
                min_size_0=1.0,
                max_size_0=1.5,
                n_alp_0=True,
            )
            for k, v in expected_fields_to_update.items():
                setattr(transformed, k, v)
            expected_on_date.append(transformed)
        expected.append(expected_on_date)
        current_date += timedelta(days=1)

    actual = list(transform(start_date, end_date, src))
    assert actual == expected


@pytest.mark.parametrize(
    "desc,start_date,end_date,raw_data_fields_to_update",
    [
        (
            "Region is skipped if type is not avalancheforecast",
            date(2000, 1, 1),
            date(2000, 1, 1),
            {"type": "other"},
        ),
    ],
)
def test_transform__invalid_regions_skipped(
    src,
    desc,
    start_date,
    end_date,
    raw_data_fields_to_update,
):
    raw_data = get_sample_raw_data(["0"])
    for entry in raw_data:
        entry |= raw_data_fields_to_update
    save_sample_raw_data(raw_data, src, start_date, end_date)

    expected = [[]]
    actual = list(transform(start_date, end_date, src))
    assert actual == expected


@pytest.mark.parametrize(
    "desc,start_date,end_date,raw_data_fields_to_update,raw_data_fields_to_remove,expection_type",
    [
        (
            "Missing required key raises KeyError",
            date(2000, 1, 1),
            date(2000, 1, 1),
            {},
            ["areaId"],
            KeyError,
        ),
        (
            "Unrecognized raw value raises KeyError",
            date(2000, 1, 1),
            date(2000, 1, 1),
            {
                "dangerRatings": {
                    "days": [{"alp": "invalid", "tln": "invalid", "btl": "invalid"}]
                }
            },
            [],
            KeyError,
        ),
    ],
)
def test_transform__malformed_raw_data_raises_exception(
    src,
    desc,
    start_date,
    end_date,
    raw_data_fields_to_update,
    raw_data_fields_to_remove,
    expection_type,
):
    raw_data = get_sample_raw_data(["0"])
    updated_raw_data = []
    for entry in raw_data:
        entry |= raw_data_fields_to_update
        entry = {k: v for k, v in entry.items() if k not in raw_data_fields_to_remove}
        updated_raw_data.append(entry)
    save_sample_raw_data(updated_raw_data, src, start_date, end_date)

    with pytest.raises(expection_type):
        list(transform(start_date, end_date, src))


@pytest.mark.parametrize(
    "desc,start_date,end_date",
    [
        (
            "Missing file raises exception",
            date(2000, 1, 1),
            date(2000, 1, 1),
        ),
    ],
)
def test_transform__missing_raw_data_raises_exception(src, desc, start_date, end_date):
    with pytest.raises(FileNotFoundError):
        list(transform(start_date, end_date, src))
