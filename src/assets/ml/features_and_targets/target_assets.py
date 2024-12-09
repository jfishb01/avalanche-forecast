import pandera as pa
from dagster import asset, AssetExecutionContext, BackfillPolicy

from src.utils.datetime_helpers import OFF_SEASON
from src.utils.schema_helpers import conform_to_schema
from src.partitions import (
    daily_region_forecast_partitions_def,
    forecast_date_and_region_id_partitions_from_key_list,
)
from src.schemas.ml.features_and_targets.target_schemas import (
    TargetSchema,
    TargetSchemaDagsterType,
)


@asset(
    io_manager_key="duck_db_io_manager",
    required_resource_keys={
        "source_data_duck_db_resource",
    },
    key_prefix="features_and_targets",
    group_name="target_creation",
    compute_kind="python",
    partitions_def=daily_region_forecast_partitions_def,
    backfill_policy=BackfillPolicy.single_run(),
    metadata={
        "partition_expr": {
            "region_id": "region_id",
            "forecast_date": "forecast_date",
        },
        "schema": "targets",
    },
    dagster_type=TargetSchemaDagsterType,
)
def target(context: AssetExecutionContext) -> pa.typing.DataFrame[TargetSchema]:
    """Use avalanche forecast center forecasts to create model targets indexed on forecast date and region id."""
    forecast_dates, region_ids = forecast_date_and_region_id_partitions_from_key_list(
        context.partition_keys
    )
    with context.resources.source_data_duck_db_resource.get_connection() as conn:
        # For now, just use problem types as our target variables. We'll add more targets to our SELECT columns
        # list as we build out more models.
        source_data_df = conn.execute(
            f"""
            SELECT
                region_id,
                forecast_date,
                problem_0,
                problem_1,
                problem_2,
                forecast_days_out,
                publish_datetime
            FROM avalanche_forecast_center.combined_avalanche_forecast_center_forecast
            WHERE forecast_date IN $forecast_dates
                AND region_id IN $region_ids
                AND avalanche_season != $off_season
            """,
            {
                "forecast_dates": forecast_dates,
                "region_ids": region_ids,
                "off_season": OFF_SEASON,
            },
        ).df()
    day_of_forecasts = source_data_df[source_data_df["forecast_days_out"] == 0].sort_values(
        by=["publish_datetime"]
    ).drop_duplicates(
        subset=["region_id", "forecast_date"], keep="last"
    )
    target_df = day_of_forecasts.assign(
        run_key=day_of_forecasts["forecast_date"].astype(str)
        + "|"
        + day_of_forecasts["region_id"],
        run_id=context.run_id,
    )
    return conform_to_schema(target_df, TargetSchema)
