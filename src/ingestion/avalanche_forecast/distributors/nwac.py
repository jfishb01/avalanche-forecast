"""Extract and transform avalanche forecasts from the Northwest Avalanche Center."""

import json
import requests
from typing import Iterable, List, Dict, Any
from datetime import date, datetime, timedelta

from src.utils.datetime_helpers import (
    date_to_avalanche_season,
    date_to_day_number_of_avalanche_season,
    date_range,
)
from src.ingestion.avalanche_forecast.ingestion_helpers import (
    publish_date_from_forecast_filename,
)
from src.schemas.feature_sets.avalanche_forecast import (
    ForecastDistributorEnum,
    RawAvalancheForecast,
    AvalancheForecastFeatureSet,
    AvalancheLikelihoodEnum,
    AvalancheRiskEnum,
)


def extract(start_date: date, end_date: date) -> Iterable[RawAvalancheForecast]:
    """Extract data from the NWAC website by making successive GET requests."""
    # NWAC forecast documents are posted to their endpoint using arbitrary IDs. In order to get the list of
    # document IDs to download, we first need to request the full list of IDs using their document index URL.
    index_url = _get_index_url(start_date, end_date)
    index_response = requests.get(index_url)
    index_response.raise_for_status()
    all_forecast_urls = index_response.json()
    all_forecast_urls_by_date = {}

    # Forecast documents are posted for each region, so group the documents by date.
    for row in all_forecast_urls:
        publish_date = datetime.fromisoformat(row["start_date"]).date()
        all_forecast_urls_by_date.setdefault(publish_date, []).append(row)

    # Return all forecast regions for a date as one record to maintain consistency with other avalanche centers.
    for publish_date in date_range(start_date, end_date):
        forecasts_for_date = []
        for forecast_region in all_forecast_urls_by_date[publish_date]:
            forecast_response = requests.get(_get_document_url(forecast_region["id"]))
            forecast_response.raise_for_status()
            forecasts_for_date.append(forecast_response.json())
        yield RawAvalancheForecast(
            publish_date=publish_date, forecast=json.dumps(forecasts_for_date)
        )


def transform(filenames: Iterable[str]) -> Iterable[List[AvalancheForecastFeatureSet]]:
    """Transform raw forecasts into a list of records, one for each forecasted region.

    This method flattens the raw JSON posted by NWAC into a list of records, one for each region. All missing
    fields are then filled with default -1 (no forecast) values and the avalanche problems are pivoted such that
    a single forecast region record has all its problems associated with it.
    """
    for filename in filenames:
        with open(filename, "r") as f:
            raw = _filter_raw(json.loads(f.read()))

        transformed = []
        publish_date = publish_date_from_forecast_filename(filename)
        observation_date = publish_date
        analysis_date = publish_date + timedelta(days=1)
        forecast_date = publish_date + timedelta(days=1)
        for region in raw:
            dangers = [x for x in region["danger"] if x["valid_day"] == "current"][0]
            summary = region["bottom_line"] or "" + region["hazard_discussion"] or ""
            transformed.append(
                AvalancheForecastFeatureSet(
                    distributor=ForecastDistributorEnum.NWAC,
                    publish_date=publish_date,
                    observation_date=observation_date,
                    publish_day_number=date_to_day_number_of_avalanche_season(
                        publish_date
                    ),
                    observation_day_number=date_to_day_number_of_avalanche_season(
                        observation_date
                    ),
                    analysis_date=analysis_date,
                    forecast_date=forecast_date,
                    analysis_day_number=date_to_day_number_of_avalanche_season(
                        analysis_date
                    ),
                    forecast_day_number=date_to_day_number_of_avalanche_season(
                        forecast_date
                    ),
                    avalanche_season=date_to_avalanche_season(publish_date),
                    area_name=region["forecast_zone"][0]["name"],
                    area_id=str(region["forecast_zone"][0]["id"]),
                    polygons=region["forecast_zone"][0]["zone_id"],
                    avalanche_summary=summary,
                    danger_alp=dangers["upper"] or AvalancheRiskEnum.NOFORECAST,
                    danger_tln=dangers["middle"] or AvalancheRiskEnum.NOFORECAST,
                    danger_btl=dangers["lower"] or AvalancheRiskEnum.NOFORECAST,
                    **_get_avalanche_problems(region["forecast_avalanche_problems"]),
                )
            )
        yield transformed


def _get_index_url(start_date: date, end_date: date) -> str:  # pragma: no cover
    """Get the URL endpoint to index all forecast documents between the start and end dates."""
    base_url = "https://api.avalanche.org/v2/public/products?avalanche_center_id=NWAC"
    return f"{base_url}&date_start={start_date.isoformat()}&date_end={(end_date + timedelta(days=1)).isoformat()}"


def _get_document_url(document_id: int) -> str:
    """Get the URL Endpoint to get a specific document using its ID."""
    return f"https://api.avalanche.org/v2/public/product/{document_id}"


def _filter_raw(raw: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    """Filters out bad records and de-duplicates raw forecast values.

    Records are discarded if they are not forecasts or if they are badly formed. If a regional forecast was posted
    multiple times, only the last forecast for the day is retained.
    """
    filtered = {}
    for region in raw:
        if (
            region["product_type"].upper() != "FORECAST"
            or len(region["forecast_zone"]) != 1
        ):
            continue

        region_id = region["forecast_zone"][0]["zone_id"]
        last_update = filtered.get(region_id, {"published_time": ""})
        if region["published_time"] > last_update["published_time"]:
            filtered[region_id] = region
    return filtered.values()


def _get_avalanche_problems(problems: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Helper method to flatten the raw forecast problems. Assumes at most 3 problems are ever posted."""
    transformed = {}
    for i, problem in enumerate(problems):
        if not problem:
            continue

        transformed |= {
            f"problem_{i}": problem["avalanche_problem_id"],
            f"likelihood_{i}": AvalancheLikelihoodEnum[
                problem["likelihood"].replace(" ", "").split("_")[0].upper()
            ],
            f"min_size_{i}": float(problem["size"][0]),
            f"max_size_{i}": float(problem["size"][1]),
            **_get_avalanche_problem_aspect_elevations(i, problem["location"]),
        }
    return transformed


def _get_avalanche_problem_aspect_elevations(
    problem_number: int, aspect_elevations: List[str]
) -> Dict[str, Any]:
    """Helper method to flatten the elevations and aspects for a given problem number."""
    elevations = {"alp": "upper", "tln": "middle", "btl": "lower"}
    aspects = {
        "n": "north",
        "ne": "northeast",
        "e": "east",
        "se": "southeast",
        "s": "south",
        "sw": "southwest",
        "w": "west",
        "nw": "northwest",
    }
    transformed = {}
    for aspect_k, aspect_v in aspects.items():
        for elevation_k, elevation_v in elevations.items():
            transformed[f"{aspect_k}_{elevation_k}_{problem_number}"] = int(
                f"{aspect_v} {elevation_v}" in aspect_elevations
            )
    return transformed
