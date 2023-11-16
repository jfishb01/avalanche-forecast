"""Extract and transform avalanche forecasts from the Colorado Avalanche Information Center."""

import pytz
import requests
from typing import Iterable
from datetime import date, datetime, timedelta, time

from src.ingestion.avalanche_forecast.types import RawAvalancheForecast


def get_url(analysis_date: date) -> str:
    datetime_str = datetime.combine(analysis_date, time(), tzinfo=pytz.UTC).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    return f"https://avalanche.state.co.us/api-proxy/avid?_api_proxy_uri=/products/all?datetime={datetime_str}"


def extract(start_date: date, end_date: date) -> Iterable[RawAvalancheForecast]:
    current_date = start_date
    while current_date <= end_date:
        response = requests.get(get_url(current_date))
        response.raise_for_status()
        yield RawAvalancheForecast(analysis_date=current_date, forecast=response.text)
        current_date += timedelta(days=1)
