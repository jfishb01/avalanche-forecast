import pandera as pa
from dagster import (
    asset,
    AssetExecutionContext,
)

from src.utils.schema_helpers import conform_to_schema
from src.partitions import (
    daily_area_forecast_partitions_def,
    forecast_date_and_area_partitions_from_key,
    forecast_center_and_area_id_from_forecast_area,
)
from src.assets.ingestion.avalanche_forecast_center_assets import (
    combined_avalanche_forecast_center_forecast,
)
from src.schemas.ml.features_and_targets.target_schemas import (
    TargetSchema,
    TargetSchemaDagsterType,
)


@asset(
    io_manager_key="duck_db_io_manager",
    required_resource_keys={
        "duck_db_resource",
    },
    key_prefix="features_and_targets",
    group_name="target_creation",
    compute_kind="python",
    partitions_def=daily_area_forecast_partitions_def,
    metadata={
        "partition_expr": {
            "forecast_area": "forecast_area",
            "forecast_date": "forecast_date",
        },
        "schema": "targets",
    },
    dagster_type=TargetSchemaDagsterType,
    deps=[combined_avalanche_forecast_center_forecast],
)
def target(context: AssetExecutionContext) -> pa.typing.DataFrame[TargetSchema]:
    """Use avalanche forecast center forecasts to create model targets indexed on forecast date and area."""
    forecast_date, forecast_area = forecast_date_and_area_partitions_from_key(
        context.partition_key
    )
    forecast_center, area_id = forecast_center_and_area_id_from_forecast_area(
        forecast_area
    )
    with context.resources.duck_db_resource.get_connection() as conn:
        # For now, just use problem types as our target variables. We'll add more targets to our SELECT columns
        # list as we build out more models.
        source_data_df = conn.execute(
            f"""
            SELECT
                problem_0,
                problem_1,
                problem_2,
                forecast_days_out
            FROM external_sources.combined_avalanche_forecast_center_forecast
            WHERE forecast_date = $forecast_date
                AND forecast_center = $forecast_center
                AND area_id = $area_id
            """,
            {
                "forecast_date": forecast_date,
                "forecast_center": forecast_center,
                "area_id": area_id,
            },
        ).df()
    target_df = source_data_df[source_data_df["forecast_days_out"] == 0].assign(
        run_key=context.partition_key,
        run_id=context.run_id,
        forecast_date=forecast_date,
        forecast_area=forecast_area,
        forecast_center=forecast_center,
        area_id=area_id,
    )
    return conform_to_schema(target_df, TargetSchema)