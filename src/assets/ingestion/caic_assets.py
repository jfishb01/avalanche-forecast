from datetime import datetime
from dagster import (
    asset,
    RetryPolicy,
    Backoff,
    Jitter,
    AssetExecutionContext,
)

from src.partitions import colorado_region_forecast_partitions_def


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
def caic_forecast_raw(context: AssetExecutionContext) -> object:  # pragma: no cover
    """Extract a CAIC avalanche forecast from the website and save it to a JSON file."""
    forecast_date = datetime.fromisoformat(context.partition_key).date()
    return context.resources.caic_resource.extract(forecast_date)
