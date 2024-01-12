"""Extract and transform avalanche forecasts from the Colorado Avalanche Information Center."""

import pytz
import json
import requests
from typing import Iterable, List, Dict, Any
from datetime import date, datetime, time, timedelta

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
    AvalancheRiskEnum,
    AvalancheLikelihoodEnum,
    AvalancheProblemEnum,
)


def extract(start_date: date, end_date: date) -> Iterable[RawAvalancheForecast]:
    """Extract the data from the CAIC website by making GET requests."""
    for publish_date in date_range(start_date, end_date):
        response = requests.get(_get_url(publish_date))
        response.raise_for_status()
        yield RawAvalancheForecast(publish_date=publish_date, forecast=response.text)


def transform(filenames: Iterable[str]) -> Iterable[List[AvalancheForecastFeatureSet]]:
    """Transform raw forecasts into a list of records, one for each forecasted region.

    CAIC posts forecasts for all regions at their endpoint. This method flattens the JSON posted into a list of
    records, one for each region. All missing fields are then filled with default -1 (no forecast) values and
    the avalanche problems are pivoted such that a single forecast region record has all its problems associated
    with it.
    """
    for filename in filenames:
        with open(filename, "r") as f:
            raw = json.loads(f.read())

        transformed = []
        publish_date = publish_date_from_forecast_filename(filename)
        observation_date = publish_date
        analysis_date = publish_date + timedelta(days=1)
        forecast_date = publish_date + timedelta(days=1)
        for region in raw:
            if region["type"].upper() != "AVALANCHEFORECAST":
                continue

            summary = ""
            if len(region["avalancheSummary"]["days"]):
                summary = region["avalancheSummary"]["days"][0]["content"]
            elevation_dangers = region["dangerRatings"]["days"][0]
            transformed.append(
                AvalancheForecastFeatureSet(
                    distributor=ForecastDistributorEnum.CAIC,
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
                    area_name=region["title"],
                    area_id=region["areaId"],
                    polygons=",".join(region["polygons"]),
                    avalanche_summary=summary,
                    danger_alp=AvalancheRiskEnum(elevation_dangers["alp"].upper()),
                    danger_tln=AvalancheRiskEnum(elevation_dangers["tln"].upper()),
                    danger_btl=AvalancheRiskEnum(elevation_dangers["btl"].upper()),
                    **_get_avalanche_problems(region["avalancheProblems"]["days"]),
                )
            )
        yield transformed


def _get_url(publish_date: date) -> str:  # pragma: no cover
    """Get the CAIC download URL. It is assumed forecasts are posted once daily at the start of the date in UTC."""
    datetime_str = datetime.combine(publish_date, time(), tzinfo=pytz.UTC).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    return f"https://avalanche.state.co.us/api-proxy/avid?_api_proxy_uri=/products/all?datetime={datetime_str}"


def _get_avalanche_problems(problems: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Helper method to flatten the raw forecast problems. Assumes at most 3 problems are ever posted."""
    transformed = {}
    for i, problem in enumerate(problems):
        if not problem:
            continue

        problem = problem[0]
        transformed |= {
            f"problem_{i}": AvalancheProblemEnum[
                problem["type"].upper().replace("DRYLOOSE", "LOOSEDRY")
            ],
            f"likelihood_{i}": AvalancheLikelihoodEnum[
                problem["likelihood"]
                .split("_")[0]
                .upper()
                .replace("CERTAIN", "ALMOSTCERTAIN")
            ],
            f"min_size_{i}": float(problem["expectedSize"]["min"]),
            f"max_size_{i}": float(problem["expectedSize"]["max"]),
            **_get_avalanche_problem_aspect_elevations(i, problem["aspectElevations"]),
        }
    return transformed


def _get_avalanche_problem_aspect_elevations(
    problem_number: int, aspect_elevations: List[str]
) -> Dict[str, Any]:
    """Helper method to flatten the elevations and aspects for a given problem number."""
    elevations = ("alp", "tln", "btl")
    aspects = ("n", "ne", "e", "se", "s", "sw", "w", "nw")
    transformed = {}
    for aspect in aspects:
        for elevation in elevations:
            transformed[f"{aspect}_{elevation}_{problem_number}"] = int(
                f"{aspect}_{elevation}" in aspect_elevations
            )
    return transformed
