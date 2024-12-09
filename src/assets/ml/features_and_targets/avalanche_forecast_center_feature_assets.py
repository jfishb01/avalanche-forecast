import pandera as pa
from dagster import asset, AssetExecutionContext, BackfillPolicy

from src.utils.datetime_helpers import OFF_SEASON
from src.utils.schema_helpers import conform_to_schema
from src.partitions import (
    daily_region_forecast_partitions_def,
    forecast_date_and_region_id_partitions_from_key_list,
)
from src.schemas.ml.features_and_targets.feature_schemas import (
    AvalancheForecastCenterForecastFeatureSchema,
    AvalancheForecastCenterForecastFeatureSchemaDagsterType,
)


@asset(
    io_manager_key="duck_db_io_manager",
    required_resource_keys={
        "source_data_duck_db_resource",
    },
    key_prefix="features_and_targets",
    group_name="feature_creation",
    compute_kind="python",
    backfill_policy=BackfillPolicy.single_run(),
    partitions_def=daily_region_forecast_partitions_def,
    metadata={
        "partition_expr": {
            "region_id": "region_id",
            "forecast_date": "forecast_date",
        },
        "schema": "features",
    },
    dagster_type=AvalancheForecastCenterForecastFeatureSchemaDagsterType,
)
def avalanche_forecast_center_feature(
    context: AssetExecutionContext,
) -> pa.typing.DataFrame[AvalancheForecastCenterForecastFeatureSchema]:
    """Use avalanche forecast center forecasts to create model features indexed on forecast date and region id.

    This lags the avalanche center forecasts by one day so that the prior day's forecast is used as a feature to
    the current day's forecast.

    DuckDB doesn't support concurrent writes, so this uses a single run backfill to significantly improve backfill
    materialization times. When deployed to a production environment with a more performant cloud-based backend,
    this should be updated to only materialize a single partition at a time.
    """
    forecast_dates, region_ids = forecast_date_and_region_id_partitions_from_key_list(
        context.partition_keys
    )
    with context.resources.source_data_duck_db_resource.get_connection() as conn:
        # Use the prior day's published forecast as features to our model
        source_data_df = (
            conn.execute(
                f"""
            SELECT 
                forecast_date + INTERVAL 1 DAY AS feature_forecast_date,
                * EXCLUDE (forecast_date, creation_datetime), 
            FROM avalanche_forecast_center.combined_avalanche_forecast_center_forecast
            WHERE forecast_date + INTERVAL 1 DAY IN $forecast_dates
                AND region_id IN $region_ids
                AND avalanche_season != $off_season
            """,
                {
                    "forecast_dates": forecast_dates,
                    "region_ids": region_ids,
                    "off_season": OFF_SEASON,
                },
            )
            .df()
            .rename(columns={"feature_forecast_date": "forecast_date"})
        )
    day_of_forecasts = source_data_df[source_data_df["forecast_days_out"] == 0].sort_values(
        by=["publish_datetime"]
    ).drop_duplicates(
        subset=["region_id", "forecast_date"], keep="last"
    )
    feature_df = day_of_forecasts.assign(
        run_key=day_of_forecasts["forecast_date"].astype(str)
        + "|"
        + day_of_forecasts["region_id"],
        run_id=context.run_id,
    )
    return conform_to_schema(feature_df, AvalancheForecastCenterForecastFeatureSchema)
