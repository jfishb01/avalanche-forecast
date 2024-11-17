import pandera as pa
from datetime import datetime
from dagster import (
    asset,
    RetryPolicy,
    Backoff,
    Jitter,
    AssetExecutionContext,
    InputContext,
    AssetKey,
)

from src.utils.schema_helpers import conform_to_schema
from src.partitions import (
    distribution_date_partitions_def,
    forecast_area_partitions_def,
    daily_avalanche_forecast_center_forecast_partitions_def,
)
from src.core.ingestion import caic
from src.schemas.ingestion.avalanche_information_center_schemas import (
    AvalancheForecastAssetSchema,
    AvalancheForecastAssetSchemaDagsterType,
)


@asset(
    io_manager_key="json_file_io_manager",
    required_resource_keys={
        "caic_resource",
    },
    key_prefix="ingestion",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60 * 60,  # hourly
        backoff=Backoff.LINEAR,
        jitter=Jitter.PLUS_MINUS,
    ),
    group_name="avalanche_forecast_center",
    compute_kind="python",
    partitions_def=distribution_date_partitions_def,
)
def raw_caic_forecast(context: AssetExecutionContext) -> object:
    """Extract a CAIC avalanche forecast from the website and save it to a JSON file."""
    forecast_date = datetime.fromisoformat(context.partition_key).date()
    return context.resources.caic_resource.extract(forecast_date)


@asset(
    io_manager_key="duck_db_io_manager",
    required_resource_keys={"json_file_io_manager"},
    key_prefix="ingestion",
    group_name="avalanche_forecast_center",
    compute_kind="python",
    partitions_def=daily_avalanche_forecast_center_forecast_partitions_def,
    metadata={
        "partition_expr": {
            "forecast_center": "forecast_center",
            "distribution_date": "distribution_date",
        },
        "schema": "external_sources",
    },
    dagster_type=AvalancheForecastAssetSchemaDagsterType,
)
def avalanche_forecast_center_forecast(
    context: AssetExecutionContext,
) -> pa.typing.DataFrame[AvalancheForecastAssetSchema]:
    """Transform the extracted avalanche forecast center file and save it to the DB.

    This asset aggregates avalanche forecast center forecasts from a number of different forecast centers by
    transforming their forecasts into a standardized form and saving them to a database table that hosts
    records from all the different forecast centers.
    """
    forecast_center = context.partition_key.keys_by_dimension["forecast_center"]
    distribution_date = context.partition_key.keys_by_dimension["distribution_date"]
    if forecast_center == "CAIC":
        raw_caic_forecast = context.resources.json_file_io_manager.load_input(
            InputContext(
                asset_key=AssetKey("ingestion/raw_caic_forecast"),
                partition_key=distribution_date,
            )
        )
        transformed = caic.transform(raw_caic_forecast).assign(
            run_id=context.run_id,
            run_key=context.partition_key,
        )
    else:
        raise ValueError(f"Unsupported forecast center: '{forecast_center}'")

    # Create partitions for all the areas included in the forecast center's forecast.
    forecast_regions = [
        f"{forecast_center}.{area_id}" for area_id in transformed["area_id"].unique()
    ]
    forecast_area_partitions_def.build_add_request(forecast_regions)
    return conform_to_schema(transformed, AvalancheForecastAssetSchema)