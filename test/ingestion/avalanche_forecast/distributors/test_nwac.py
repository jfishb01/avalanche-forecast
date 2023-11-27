import os
import json
import uuid
import pytest
import shutil
from datetime import date, datetime
from unittest.mock import patch
from requests import HTTPError
from typing import Iterable, List, Dict, Any

from src.utils.datetime_helpers import date_range
from src.ingestion.avalanche_forecast.distributors.nwac import extract, transform
from src.ingestion.avalanche_forecast.common import (
    forecast_filename,
    RawAvalancheForecast,
    TransformedAvalancheForecast,
    ForecastDistributorEnum,
    AvalancheRiskEnum,
    AvalancheProblemEnum,
    AvalancheLikelihoodEnum,
)


def get_sample_raw_data(
    region_ids: Iterable[str], use_unique_zone_ids=True
) -> List[Dict[str, Any]]:
    raw_data = []
    for id in region_ids:
        zone_id = f"zone_id_{id}" if use_unique_zone_ids else "zone_id"
        raw_data.append(
            {
                "title": f"title_{id}",
                "published_time": datetime(2000, 1, 1, int(id)).isoformat(),
                "product_type": "forecast",
                "forecast_zone": [
                    {
                        "name": f"zone_name_{id}",
                        "id": f"id_{id}",
                        "zone_id": zone_id,
                        "published_time": datetime(2000, 1, 1, int(id)).isoformat(),
                    }
                ],
                "bottom_line": "bottom line",
                "hazard_discussion": "hazard discussion",
                "danger": [
                    {"valid_day": "current", "upper": 1, "middle": 2, "lower": 3}
                ],
                "forecast_avalanche_problems": [
                    {
                        "avalanche_problem_id": 1,
                        "likelihood": "almost certain",
                        "size": [1.5, 2.5],
                        "location": ["north upper"],
                    }
                ],
            }
        )
    return raw_data


def generate_sample_raw_files(
    raw_data: List[Dict[str, Any]], dir: str, start_date: date, end_date: date
) -> Iterable[str]:
    raw_data_filenames = [
        forecast_filename(ForecastDistributorEnum.NWAC, d, dir)
        for d in date_range(start_date, end_date)
    ]
    for filename in raw_data_filenames:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as f:
            f.write(json.dumps(raw_data))
    return raw_data_filenames


@pytest.fixture
def src():
    src = f"/tmp/test/{str(uuid.uuid4())}"
    os.makedirs(src, exist_ok=True)
    yield src
    shutil.rmtree(src, ignore_errors=True)


@pytest.fixture
def mock_response():
    class Response:
        def __init__(self, text: str = "", raise_exception=False):
            self.text = text
            self._raise_exception = raise_exception

        def json(self):
            return json.loads(self.text)

        def raise_for_status(self):
            if self._raise_exception:
                raise HTTPError

    def generate_response(url):
        if "invalid" in url:
            return Response(raise_exception=True)
        if "document_index" in url:
            return Response(text=url.split("params=")[1])
        return Response(text=json.dumps(url.split("/")[-1]))

    with patch("requests.get", side_effect=generate_response) as m:
        yield m


@pytest.mark.parametrize(
    "desc,start_date,end_date,document_index",
    [
        (
            "Results extracted for each date with single region",
            date(2000, 1, 1),
            date(2000, 1, 2),
            [
                {"start_date": "2000-01-01", "id": "0"},
                {"start_date": "2000-01-02", "id": "1"},
            ],
        ),
        (
            "Results extracted for each date with multiple regions",
            date(2000, 1, 1),
            date(2000, 1, 2),
            [
                {"start_date": "2000-01-01", "id": "0"},
                {"start_date": "2000-01-01", "id": "1"},
                {"start_date": "2000-01-02", "id": "2"},
            ],
        ),
        (
            "Results extracted for each date with multiple regions, document index date order does not matter",
            date(2000, 1, 1),
            date(2000, 1, 2),
            [
                {"start_date": "2000-01-01", "id": "0"},
                {"start_date": "2000-01-02", "id": "1"},
                {"start_date": "2000-01-01", "id": "2"},
            ],
        ),
    ],
)
def test_extract(mock_response, desc, start_date, end_date, document_index):
    expected = []
    for analysis_date in date_range(start_date, end_date):
        region_forecasts = [
            d["id"]
            for d in document_index
            if d["start_date"] == analysis_date.isoformat()
        ]
        expected.append(
            RawAvalancheForecast(
                analysis_date=analysis_date, forecast=json.dumps(region_forecasts)
            )
        )
    with patch(
        "src.ingestion.avalanche_forecast.distributors.nwac._get_index_url",
        return_value=f"https://document_index?params={json.dumps(document_index)}",
    ):
        actual = list(extract(start_date, end_date))
    assert actual == expected


def test_extract__bad_index_request_raises_error(mock_response):
    with patch(
        f"src.ingestion.avalanche_forecast.distributors.nwac._get_index_url",
        return_value="invalid",
    ):
        with pytest.raises(HTTPError):
            list(extract(date(2000, 1, 1), date(2000, 1, 2)))


def test_extract__bad_document_request_raises_error(mock_response):
    document_index = [{"start_date": "2000-01-01", "id": "0"}]
    with patch(
        "src.ingestion.avalanche_forecast.distributors.nwac._get_index_url",
        return_value=f"https://document_index?params={json.dumps(document_index)}",
    ):
        with patch(
            f"src.ingestion.avalanche_forecast.distributors.nwac._get_document_url",
            return_value="invalid",
        ):
            with pytest.raises(HTTPError):
                list(extract(date(2000, 1, 1), date(2000, 1, 1)))


def test_extract__no_region_on_date_raises_error(mock_response):
    document_index = [{"start_date": "2000-01-02", "id": "0"}]
    with patch(
        "src.ingestion.avalanche_forecast.distributors.nwac._get_index_url",
        return_value=f"https://document_index?params={json.dumps(document_index)}",
    ):
        with pytest.raises(KeyError):
            list(extract(date(2000, 1, 1), date(2000, 2, 1)))


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
            "No problems still works",
            date(2000, 1, 1),
            date(2000, 1, 1),
            ["0"],
            {"forecast_avalanche_problems": []},
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
    raw_filenames = generate_sample_raw_files(raw_data, src, start_date, end_date)

    expected = []
    for analysis_date in date_range(start_date, end_date):
        expected_on_date = []
        for id in region_ids:
            transformed = TransformedAvalancheForecast(
                distributor=ForecastDistributorEnum.NWAC,
                analysis_date=analysis_date,
                forecast_date=analysis_date,
                avalanche_season="1999/2000",
                area_name=f"zone_name_{id}",
                area_id=f"id_{id}",
                polygons=f"zone_id_{id}",
                avalanche_summary=f"bottom line",
                danger_alp=AvalancheRiskEnum.LOW,
                danger_tln=AvalancheRiskEnum.MODERATE,
                danger_btl=AvalancheRiskEnum.CONSIDERABLE,
                problem_0=AvalancheProblemEnum.LOOSEDRY,
                likelihood_0=AvalancheLikelihoodEnum.CERTAIN,
                min_size_0=1.5,
                max_size_0=2.5,
                n_alp_0=True,
            )
            for k, v in expected_fields_to_update.items():
                setattr(transformed, k, v)
            expected_on_date.append(transformed)
        expected.append(expected_on_date)

    actual = list(transform(raw_filenames))
    assert actual == expected


@pytest.mark.parametrize(
    "desc,start_date,end_date,raw_data_fields_to_update",
    [
        (
            "Region is skipped if type is not avalancheforecast",
            date(2000, 1, 1),
            date(2000, 1, 1),
            {"product_type": "other"},
        ),
        (
            "Forecast skipped if multiple zones present",
            date(2000, 1, 1),
            date(2000, 1, 1),
            {"forecast_zone": [{}, {}]},
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
    raw_filenames = generate_sample_raw_files(raw_data, src, start_date, end_date)

    expected = [[]]
    actual = list(transform(raw_filenames))
    assert actual == expected


@pytest.mark.parametrize(
    "desc,start_date,end_date,raw_data_fields_to_update,raw_data_fields_to_remove,expection_type",
    [
        (
            "Missing required key raises KeyError",
            date(2000, 1, 1),
            date(2000, 1, 1),
            {},
            ["product_type"],
            KeyError,
        ),
        (
            "Unrecognized raw value raises KeyError",
            date(2000, 1, 1),
            date(2000, 1, 1),
            {
                "forecast_avalanche_problems": [
                    {
                        "avalanche_problem_id": 1,
                        "likelihood": "invalid",
                    }
                ],
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
    raw_filenames = generate_sample_raw_files(
        updated_raw_data, src, start_date, end_date
    )

    with pytest.raises(expection_type):
        list(transform(raw_filenames))


def test_transform__latest_published_region_used(src):
    analysis_date = date(2000, 1, 1)
    raw_data = get_sample_raw_data(["0", "1"], use_unique_zone_ids=False)
    raw_data[0]["forecast_zone"][0]["published_time"] = datetime(
        2000, 1, 1, 5
    ).isoformat()
    raw_filenames = generate_sample_raw_files(
        raw_data, src, analysis_date, analysis_date
    )

    expected = [
        [
            TransformedAvalancheForecast(
                distributor=ForecastDistributorEnum.NWAC,
                analysis_date=analysis_date,
                forecast_date=analysis_date,
                avalanche_season="1999/2000",
                area_name=f"zone_name_1",
                area_id=f"id_1",
                polygons=f"zone_id",
                avalanche_summary=f"bottom line",
                danger_alp=AvalancheRiskEnum.LOW,
                danger_tln=AvalancheRiskEnum.MODERATE,
                danger_btl=AvalancheRiskEnum.CONSIDERABLE,
                problem_0=AvalancheProblemEnum.LOOSEDRY,
                likelihood_0=AvalancheLikelihoodEnum.CERTAIN,
                min_size_0=1.5,
                max_size_0=2.5,
                n_alp_0=True,
            )
        ]
    ]
    actual = list(transform(raw_filenames))
    assert actual == expected
