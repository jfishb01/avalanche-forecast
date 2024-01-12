"""Helper methods to support the ML model generation and inference."""

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame
from datetime import date, timedelta
from typing import Dict, Union, Sequence

from src.schemas.ml.forecast_model import (
    ModelFeatureSchema,
    ModelTargetSchema,
)
from src.ingestion.avalanche_forecast.ingestion_helpers import ForecastDistributorEnum
from src.utils.datetime_helpers import (
    date_range,
    date_to_avalanche_season,
    date_to_day_number_of_avalanche_season,
    get_avalanche_season_date_bounds,
)


@pa.check_types
def get_targets(
    db_uri: str,
    distributors: Sequence[ForecastDistributorEnum],
    start_analysis_date: date,
    end_analysis_date: date,
    target: str,
) -> DataFrame[ModelTargetSchema]:
    """Get model targets over the provided date range and distributors."""
    return pd.read_sql(
        f"""
            SELECT
                distributor, 
                area_id, 
                area_name, 
                observation_date,
                {target} as target
            FROM external.avalanche_forecast
            WHERE 
                distributor IN %(distributors)s
                AND dateInAvalancheSeason(observation_date)
                AND greaterOrEquals(analysis_date, %(start_analysis_date)s)
                AND lessOrEquals(analysis_date, %(end_analysis_date)s)
            ORDER BY avalanche_season, distributor, area_id, analysis_day_number        
            """,
        db_uri,
        params=dict(
            distributors=distributors,
            start_analysis_date=start_analysis_date,
            end_analysis_date=end_analysis_date,
        ),
    )


@pa.check_types
def get_features(
    db_uri: str,
    distributors: Sequence[ForecastDistributorEnum],
    start_analysis_date: date,
    end_analysis_date: date,
    features: Sequence[str],
    fill_values: Union[int, float, Dict[str, Union[int, float]]] = 0,
) -> DataFrame[ModelFeatureSchema]:
    """Get model features over the provided date range and distributors, imputing any necessary values.

    Args:
        db_uri: The URI of the database backend.
        distributors: Distributors to use for grabbing features.
        start_analysis_date: The start analysis date to use.
        end_analysis_date: The end analysis date to use.
        features: Set of features to retrieve
        fill_values: Values to use to fill any missing values in the dataset. If a scalar is provided,
            all values will be filled with the scalar. If a dict, is provided, matching columns will be filled
            with the corresponding dictionary values. Any unmatched columns will be filled with 0.

    Returns: The feature set for the distributors over the provided date range.
    """
    start_avalanche_season = date_to_avalanche_season(
        start_analysis_date, use_upcoming_when_unmapped=True
    )
    start_analysis_date = max(
        start_analysis_date, get_avalanche_season_date_bounds(start_avalanche_season)[0]
    )
    forecast_area_fields = {
        "distributor",
        "area_name",
        "area_id",
    }
    forecast_date_fields = {
        "analysis_date",
        "forecast_date",
    }
    features_subset = set(features) - forecast_area_fields - forecast_date_fields
    feature_df = pd.read_sql(
        f"""
            WITH all_forecast_areas AS (
                SELECT
                    distributor,
                    MAX(area_name) as area_name,
                    area_id
                FROM external.avalanche_forecast
                WHERE
                    distributor IN %(distributors)s
                GROUP BY distributor, area_id
            ),
            max_analysis_dates_up_to_start AS (
                SELECT
                    distributor,
                    area_id,
                    MAX(analysis_date) AS max_analysis_date
                FROM external.avalanche_forecast
                WHERE
                    distributor IN %(distributors)s
                    AND dateInAvalancheSeason(publish_date)
                    AND dateInAvalancheSeason(analysis_date)
                    AND dateInAvalancheSeason(forecast_date)
                    AND lessOrEquals(analysis_date, %(start_analysis_date)s)
                    AND avalanche_season = %(start_avalanche_season)s
                GROUP BY distributor, area_id
            )

            -- Get the start entry, forward filling any prior entries if necessary
            SELECT
                areas.area_id AS area_id,
                areas.area_name AS area_name,
                areas.distributor AS distributor,
                %(start_analysis_date)s::date AS analysis_date,
                %(start_analysis_date)s::date AS forecast_date,
                {",".join([f'features.{field} AS {field}' for field in sorted(features_subset)])}
            FROM all_forecast_areas areas
            LEFT JOIN max_analysis_dates_up_to_start max_analysis_dates
                ON max_analysis_dates.distributor = areas.distributor
                AND max_analysis_dates.area_id = areas.area_id
            LEFT JOIN external.avalanche_forecast features
                ON features.distributor = max_analysis_dates.distributor
                AND features.area_id = max_analysis_dates.area_id
                AND max_analysis_dates.max_analysis_date = features.analysis_date

            UNION ALL

            -- Get all entries after the start date up to the end date
            SELECT
                {",".join([f"features.{field} AS {field}" for field in sorted(forecast_area_fields)])},
                {",".join([f"features.{field} AS {field}" for field in sorted(forecast_date_fields)])},
                {",".join([f"features.{field} AS {field}" for field in sorted(features_subset)])}
            FROM external.avalanche_forecast features
            INNER JOIN all_forecast_areas areas
                ON features.distributor = areas.distributor
                AND features.area_id = areas.area_id
            WHERE
                distributor IN %(distributors)s
                AND dateInAvalancheSeason(publish_date)
                AND dateInAvalancheSeason(analysis_date)
                AND dateInAvalancheSeason(forecast_date)
                AND greater(analysis_date, %(start_analysis_date)s)
                AND lessOrEquals(analysis_date, %(end_analysis_date)s)           
        """,
        db_uri,
        params=dict(
            distributors=distributors,
            start_avalanche_season=start_avalanche_season,
            start_analysis_date=start_analysis_date,
            end_analysis_date=end_analysis_date,
        ),
    )
    return _impute_feature_entries(
        feature_df, start_analysis_date, end_analysis_date, fill_values
    )


def _impute_feature_entries(
    feature_df: DataFrame[ModelFeatureSchema],
    start_analysis_date: date,
    end_analysis_date: date,
    fill_values: Union[int, float, Dict[str, Union[int, float]]] = 0,
) -> DataFrame[ModelFeatureSchema]:
    def ffill_missing_dates(df):
        avalanche_season = df["avalanche_season"].iloc[0]
        season_start_date, season_end_date = get_avalanche_season_date_bounds(
            avalanche_season
        )

        # Add empty entries for the day before the season starts and forecast up to the last day of the season
        expected_dates_index = pd.Series(
            name=reindex_col,
            data=date_range(
                max(start_analysis_date, season_start_date - timedelta(days=1)),
                min(end_analysis_date, season_end_date - timedelta(days=1)),
            ),
        )
        return (
            df.set_index(reindex_col)
            .reindex(expected_dates_index)
            .ffill()
            .fillna(
                {
                    "avalanche_season": avalanche_season,
                    "distributor": df["distributor"].iloc[0],
                    "area_name": df["area_name"].iloc[0],
                    "area_id": df["area_id"].iloc[0],
                }
            )
            .assign(forecast_date=expected_dates_index.tolist())
            .fillna(fill_values)
            .fillna(0)
            .reset_index()
        )

    reindex_col = "analysis_date"

    output_cols = feature_df.columns
    if "avalanche_season" not in feature_df.columns:
        feature_df["avalanche_season"] = feature_df["analysis_date"].apply(
            date_to_avalanche_season
        )
    imputed_df = (
        feature_df.groupby(["distributor", "area_id", "avalanche_season"])
        .apply(ffill_missing_dates)
        .reset_index(drop=True)
    )
    day_number_cols = [col for col in imputed_df.columns if col.endswith("_day_number")]
    for col in day_number_cols:
        imputed_df[col] = imputed_df[col.replace("_day_number", "_date")].apply(
            date_to_day_number_of_avalanche_season
        )

    return imputed_df[
        (imputed_df[reindex_col] >= start_analysis_date)
        & (imputed_df[reindex_col] <= end_analysis_date)
    ][output_cols]
