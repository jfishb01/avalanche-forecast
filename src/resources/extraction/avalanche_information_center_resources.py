import pytz
import requests
from typing import List, Dict, Any
from datetime import date, datetime, time, timedelta
from dagster import ConfigurableResource


class CAICResource(ConfigurableResource):
    """Resource for extracting avalanche forecasts from the Colorado Avalanche Information Center."""

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


class NWACResource(ConfigurableResource):
    """Resource for extracting avalanche forecasts from the Northwest Avalanche Center."""

    def extract(self, forecast_date: date) -> List[Dict[str, Any]]:
        """Extract a json forecast from the NWAC site for the provided forecast_date."""

        # NWAC forecast documents are posted to their endpoint using arbitrary IDs. In order to get the list of
        # document IDs to download, we first need to request the full list of IDs using their document index URL.
        documents_to_extract_response = requests.get(
            "https://api.avalanche.org/v2/public/products",
            params=dict(
                avalanche_center_id="NWAC",
                date_start=forecast_date,
                date_end=forecast_date + timedelta(days=1),
            ),
        )
        documents_to_extract_response.raise_for_status()
        document_urls = documents_to_extract_response.json()

        # Return all forecast regions for a date as one record to maintain consistency with other avalanche centers.
        region_forecasts = []
        for document_url in document_urls:
            region_forecast_response = requests.get(
                f"https://api.avalanche.org/v2/public/product/{document_url['id']}"
            )
            region_forecast_response.raise_for_status()
            region_forecasts.append(region_forecast_response.json())
        return region_forecasts
