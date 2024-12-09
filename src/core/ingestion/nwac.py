import pytz
import pandas as pd
import pandera as pa
from datetime import datetime
from typing import Any

from src.utils.schema_helpers import conform_to_schema
from src.utils.datetime_helpers import (
    date_to_avalanche_season,
    date_to_day_number_of_avalanche_season,
)
from src.schemas.ingestion.avalanche_forecast_center_schemas import (
    AvalancheForecastCenterForecastSchema,
    AvalancheLikelihoodEnum,
)

DATETIME_FMTS = [
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%fZ",
]
REPORT_TIMEZONE = pytz.timezone("America/Los_Angeles")


def _map_aspect_elevation_to_schema_field(aspect_elevation: str) -> str:
    """NWAC uses different language for aspect elevations than is defined in our schema, so we need to map them."""
    aspect_elevation_map = {
        "north": "n",
        "northeast": "ne",
        "east": "e",
        "southeast": "se",
        "south": "s",
        "southwest": "sw",
        "west": "w",
        "northwest": "nw",
        "upper": "alp",
        "middle": "tln",
        "lower": "btl",
    }
    for k, v in aspect_elevation_map.items():
        aspect_elevation = aspect_elevation.replace(k, v).replace(" ", "_")
    return aspect_elevation


def transform(
    raw_nwac_forecast: Any,
) -> pa.typing.DataFrame[AvalancheForecastCenterForecastSchema]:
    """Transform raw forecasts into a list of records, one for each forecasted region for each day forecasted out.

    NWAC posts forecasts for all regions at their endpoint. This method flattens the JSON posted into a dataframe
    of records, one for each region and forecast day out. All missing fields are filled with default 0 (no
    forecast) values and the avalanche problems are pivoted such that each region/forecast day record has all
    its problems associated with it.
    """
    transformed = []
    # Iterate through the list of regions forecasted
    for region_group in raw_nwac_forecast:
        if region_group["product_type"].upper() != "FORECAST":
            continue
        distribution_date = (
            datetime.fromisoformat(region_group["published_time"])
            .astimezone(REPORT_TIMEZONE)
            .date()
        )
        analysis_datetime = datetime.fromisoformat(region_group["updated_at"])
        forecast_date = distribution_date
        elevation_dangers = [
            danger
            for danger in region_group["danger"]
            if danger["valid_day"] == "current"
        ][0]
        region_forecast = dict(
            forecast_center="NWAC",
            publish_datetime=analysis_datetime,
            analysis_datetime=analysis_datetime,
            distribution_date=distribution_date,
            forecast_date=distribution_date,
            forecast_days_out=0,
            forecast_date_season_day_number=date_to_day_number_of_avalanche_season(
                forecast_date
            ),
            avalanche_season=date_to_avalanche_season(forecast_date),
            avalanche_summary=region_group["bottom_line"]
            or "" + region_group["hazard_discussion"]
            or "",
            danger_alp=elevation_dangers["upper"],
            danger_tln=elevation_dangers["middle"],
            danger_btl=elevation_dangers["lower"],
        )
        # Iterate through all the problems for the forecast date, skipping any that are empty (they'll be
        # filled with a NO FORECAST later).
        for problem in region_group["forecast_avalanche_problems"]:
            if not problem:
                continue
            problem_number = problem["rank"] - 1
            region_forecast |= {
                f"problem_{problem_number}": problem["avalanche_problem_id"],
                f"likelihood_{problem_number}": AvalancheLikelihoodEnum[
                    problem["likelihood"].replace(" ", "").upper()
                ],
                f"min_size_{problem_number}": float(problem["size"][0]),
                f"max_size_{problem_number}": float(problem["size"][1]),
            }

            # Only aspects/elevations that are associated with the problem are listed in the JSON so set a
            # bit flag for each of them so they don't get filled with a NO FORECAST.
            aspect_elevations_observed_for_problem = {
                f"{_map_aspect_elevation_to_schema_field(aspect_elevation)}_{problem_number}": 1
                for aspect_elevation in problem["location"]
            }
            region_forecast |= aspect_elevations_observed_for_problem

            # Sum up the problematic aspects to get the prevalence at each elevation and across all elevations
            problem_prevalence = {}
            for elevation in ("alp", "tln", "btl"):
                problem_prevalence[f"{elevation}_prevalence_{problem_number}"] = len(
                    [
                        k
                        for k in aspect_elevations_observed_for_problem.keys()
                        if elevation in k
                    ]
                )
            problem_prevalence[f"prevalence_{problem_number}"] = sum(
                problem_prevalence.values()
            )
            region_forecast |= problem_prevalence

        for region in region_group["forecast_zone"]:
            region_forecast |= dict(
                area_name=region["name"],
                area_id=str(region["id"]),
                polygons=region["zone_id"],
                region_id=f"NWAC.{region['id']}",
            )
        transformed.append(region_forecast)
    return conform_to_schema(
        pd.DataFrame(transformed), AvalancheForecastCenterForecastSchema
    )
