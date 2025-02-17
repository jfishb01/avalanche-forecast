import pytz
import pandas as pd
import pandera as pa
from datetime import timedelta
from typing import Any

from src.utils.ingestion_helpers import try_get_field
from src.utils.schema_helpers import conform_to_schema
from src.utils.datetime_helpers import (
    try_strptime,
    date_to_avalanche_season,
    date_to_day_number_of_avalanche_season,
)
from src.schemas.ingestion.avalanche_forecast_center_schemas import (
    AvalancheForecastCenterForecastSchema,
    AvalancheRiskEnum,
    AvalancheLikelihoodEnum,
    AvalancheProblemEnum,
)

DATETIME_FMTS = [
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%fZ",
]
REPORT_TIMEZONE = pytz.timezone("America/Denver")


def transform(
    raw_caic_forecast: Any,
) -> pa.typing.DataFrame[AvalancheForecastCenterForecastSchema]:
    """Transform raw forecasts into a list of records, one for each forecasted region for each day forecasted out.

    CAIC posts forecasts for all regions at their endpoint. This method flattens the JSON posted into a dataframe
    of records, one for each region and forecast day out. All missing fields are filled with default 0 (no
    forecast) values and the avalanche problems are pivoted such that each region/forecast day record has all
    its problems associated with it.
    """
    transformed = []
    # Iterate through the list of regions forecasted
    for region in raw_caic_forecast:
        if region["type"].upper() != "AVALANCHEFORECAST":
            continue

        # Forecasts are posted for a named date then indexed off of that for each subsequent forecast day out
        distribution_date = REPORT_TIMEZONE.localize(
            try_strptime(region["expiryDateTime"], DATETIME_FMTS)
        ).date()
        analysis_datetime = REPORT_TIMEZONE.localize(
            try_strptime(region["issueDateTime"], DATETIME_FMTS)
        ).astimezone(pytz.UTC)

        # Forecasts for each day out are posted as lists under multiple keys with potentially varying lengths.
        # We use whatever list has the most days forecasted out and fill any missing values with NO FORECAST.
        forecast_days_out = max(
            len(region["dangerRatings"]["days"]),
            len(region["avalancheProblems"]["days"]),
        )
        for day in range(forecast_days_out):
            forecast_date = distribution_date + timedelta(days=day)
            summary = try_get_field(
                region["avalancheSummary"]["days"], (day, "content"), ""
            )
            elevation_dangers = try_get_field(
                region["dangerRatings"]["days"], (day,), {}
            )
            region_forecast = dict(
                region_id=f"CAIC.{region['areaId']}",
                forecast_center="CAIC",
                publish_datetime=analysis_datetime,
                analysis_datetime=analysis_datetime,
                distribution_date=distribution_date,
                forecast_date=forecast_date,
                forecast_days_out=day,
                forecast_date_season_day_number=date_to_day_number_of_avalanche_season(
                    forecast_date
                ),
                avalanche_season=date_to_avalanche_season(forecast_date),
                area_name=region.get("title"),
                area_id=region["areaId"],
                polygons=",".join(region["polygons"]),
                avalanche_summary=summary,
                danger_alp=AvalancheRiskEnum(elevation_dangers.get("alp", "").upper()),
                danger_tln=AvalancheRiskEnum(elevation_dangers.get("tln", "").upper()),
                danger_btl=AvalancheRiskEnum(elevation_dangers.get("btl", "").upper()),
            )

            forecasted_problems = try_get_field(
                region["avalancheProblems"]["days"], (day,), []
            )
            # Iterate through all the problems for the forecast date, skipping any that are empty (they'll be
            # filled with a NO FORECAST later).
            for problem_number, problem in enumerate(forecasted_problems):
                if not problem:
                    continue

                # Standardize some field values that have been observed to not conform to known keys.
                region_forecast |= {
                    f"problem_type_{problem_number}": AvalancheProblemEnum[
                        problem["type"].upper().replace("DRYLOOSE", "LOOSEDRY")
                    ],
                    f"likelihood_{problem_number}": AvalancheLikelihoodEnum[
                        problem["likelihood"]
                        .split("_")[0]
                        .upper()
                        .replace("CERTAIN", "ALMOSTCERTAIN")
                    ],
                    f"min_size_{problem_number}": float(problem["expectedSize"]["min"]),
                    f"max_size_{problem_number}": float(problem["expectedSize"]["max"]),
                }

                # Only aspects/elevations that are associated with the problem are listed in the JSON so set a
                # bit flag for each of them so they don't get filled with a NO FORECAST.
                aspect_elevations_observed_for_problem = {
                    f"{aspect_elevation}_{problem_number}": 1
                    for aspect_elevation in problem["aspectElevations"]
                }
                region_forecast |= aspect_elevations_observed_for_problem

                # Sum up the problematic aspects to get the prevalence at each elevation and across all elevations
                problem_prevalence = {}
                for elevation in ("alp", "tln", "btl"):
                    problem_prevalence[f"{elevation}_prevalence_{problem_number}"] = (
                        len(
                            [
                                k
                                for k in aspect_elevations_observed_for_problem.keys()
                                if elevation in k
                            ]
                        )
                    )
                problem_prevalence[f"prevalence_{problem_number}"] = sum(
                    problem_prevalence.values()
                )
                region_forecast |= problem_prevalence
            transformed.append(region_forecast)
    return conform_to_schema(
        pd.DataFrame(transformed), AvalancheForecastCenterForecastSchema
    )
