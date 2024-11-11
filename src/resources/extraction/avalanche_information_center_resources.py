import pytz
import requests
from typing import Dict, Any
from datetime import date, datetime, time
from dagster import ConfigurableResource


class CAICResource(ConfigurableResource):
    """Resource for extracting avalanche forecasts from  the Colorado Avalanche Information Center."""

    def extract(self, forecast_date: date) -> Dict[str, Any]:
        """Extract a json forecast from the CAIC site for the provided forecast_date."""
        forecast_datetime_str = datetime.combine(
            forecast_date, time(), tzinfo=pytz.UTC
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        response = requests.get(
            f"https://avalanche.state.co.us/api-proxy/avid?_api_proxy_uri=/products/all?datetime={forecast_datetime_str}"
        )
        response.raise_for_status()
        return response.json()
