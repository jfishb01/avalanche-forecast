import pandera as pa
from datetime import datetime
from dagster import (
    asset,
    RetryPolicy,
    Backoff,
    Jitter,
    AssetExecutionContext,
    AssetIn,
)

from src.utils.schema_helpers import conform_to_schema
from src.partitions import colorado_region_forecast_partitions_def
from src.core.ingestion.caic import flatten_to_regional_forecast_days_df
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
    group_name="CAIC",
    compute_kind="python",
    partitions_def=colorado_region_forecast_partitions_def,
)
def raw_caic_forecast(context: AssetExecutionContext) -> object:
    """Extract a CAIC avalanche forecast from the website and save it to a JSON file."""
    forecast_date = datetime.fromisoformat(context.partition_key).date()
    return context.resources.caic_resource.extract(forecast_date)


@asset(
    io_manager_key="duck_db_io_manager",
    required_resource_keys={
        "caic_resource",
    },
    ins={
        "raw_caic_forecast": AssetIn(
            key=raw_caic_forecast.key,
            input_manager_key="json_file_io_manager",
        ),
    },
    key_prefix="ingestion",
    group_name="CAIC",
    compute_kind="python",
    partitions_def=colorado_region_forecast_partitions_def,
    metadata={
        "partition_expr": "analysis_datetime",
        "schema": "external_sources",
    },
    dagster_type=AvalancheForecastAssetSchemaDagsterType,
)
def caic_forecast(
    context: AssetExecutionContext, raw_caic_forecast: object
) -> pa.typing.DataFrame[AvalancheForecastAssetSchema]:
    """Transform the raw CAIC avalanche forecast JSON and save it to the DB."""
    transformed = flatten_to_regional_forecast_days_df(raw_caic_forecast).assign(
        run_id=context.run_id,
        run_key=context.partition_key,
    )
    return conform_to_schema(transformed, AvalancheForecastAssetSchema)
