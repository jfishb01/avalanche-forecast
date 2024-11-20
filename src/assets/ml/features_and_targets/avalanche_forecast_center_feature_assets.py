import pandera as pa
from datetime import timedelta
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
from src.schemas.ml.features_and_targets.feature_schemas import (
    AvalancheForecastCenterForecastFeatureSchema,
    AvalancheForecastCenterForecastFeatureSchemaDagsterType,
)


@asset(
    io_manager_key="duck_db_io_manager",
    required_resource_keys={
        "duck_db_resource",
    },
    key_prefix="features_and_targets",
    group_name="feature_creation",
    compute_kind="python",
    partitions_def=daily_area_forecast_partitions_def,
    metadata={
        "partition_expr": {
            "forecast_area": "forecast_area",
            "forecast_date": "forecast_date",
        },
        "schema": "features",
    },
    dagster_type=AvalancheForecastCenterForecastFeatureSchemaDagsterType,
    deps=[combined_avalanche_forecast_center_forecast],
)
def avalanche_forecast_center_feature(
    context: AssetExecutionContext,
) -> pa.typing.DataFrame[AvalancheForecastCenterForecastFeatureSchema]:
    """Use avalanche forecast center forecasts to create model features indexed on forecast date and area."""
    forecast_date, forecast_area = forecast_date_and_area_partitions_from_key(
        context.partition_key
    )
    forecast_center, area_id = forecast_center_and_area_id_from_forecast_area(
        forecast_area
    )

    # Use the prior day's published forecast as features to our model
    distribution_date = forecast_date - timedelta(days=1)
    with context.resources.duck_db_resource.get_connection() as conn:
        source_data_df = conn.execute(
            f"""
            SELECT * EXCLUDE(creation_datetime)
            FROM external_sources.combined_avalanche_forecast_center_forecast
            WHERE distribution_date = $distribution_date
                AND forecast_center = $forecast_center
                AND area_id = $area_id
            """,
            {
                "distribution_date": distribution_date,
                "forecast_center": forecast_center,
                "area_id": area_id,
            },
        ).df()
    feature_df = source_data_df[source_data_df["forecast_days_out"] == 0].assign(
        run_key=context.partition_key,
        run_id=context.run_id,
        forecast_date=forecast_date,
        forecast_area=forecast_area,
        forecast_center=forecast_center,
        area_id=area_id,
    )
    return conform_to_schema(feature_df, AvalancheForecastCenterForecastFeatureSchema)
