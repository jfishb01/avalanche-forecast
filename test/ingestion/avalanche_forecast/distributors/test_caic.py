import pytest
from datetime import date, timedelta
from unittest.mock import patch
from requests import HTTPError

from src.ingestion.avalanche_forecast.distributors.caic import extract, get_url
from src.ingestion.avalanche_forecast.custom_types import RawAvalancheForecast


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
        "src.ingestion.avalanche_forecast.distributors.caic.get_url",
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
        "src.ingestion.avalanche_forecast.distributors.caic.get_url",
        return_value="invalid",
    ):
        with pytest.raises(HTTPError):
            list(extract(start_date, end_date))


@pytest.mark.parametrize(
    "desc,analysis_date",
    [("URL created with analysis datetime parameter", date(2000, 1, 1))],
)
def test_get_url(desc, analysis_date):
    expected = "https://avalanche.state.co.us/api-proxy/avid?_api_proxy_uri=/products/all?datetime=2000-01-01T00:00:00Z"
    actual = get_url(analysis_date)
    assert actual == expected
