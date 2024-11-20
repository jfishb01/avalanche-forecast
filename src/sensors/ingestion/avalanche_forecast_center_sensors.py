from dagster import (
    RunRequest,
    EventLogEntry,
    SensorEvaluationContext,
    asset_sensor,
    MultiPartitionKey,
)

from src.jobs import combined_avalanche_forecast_center_forecast_job
from src.assets.ingestion.avalanche_forecast_center_assets import (
    raw_caic_forecast,
)


@asset_sensor(
    asset_key=raw_caic_forecast.key, job=combined_avalanche_forecast_center_forecast_job
)
def raw_caic_forecast_materialization_sensor(
    context: SensorEvaluationContext, asset_event: EventLogEntry
) -> RunRequest:
    """Listen for materialization events of extracted CAIC forecasts and add them to the combined forecast table"""
    assert (
        asset_event.dagster_event
        and asset_event.dagster_event.asset_key
        and asset_event.dagster_event.partition
    )
    yield RunRequest(
        partition_key=MultiPartitionKey(
            {
                "forecast_center": "CAIC",
                "distribution_date": asset_event.dagster_event.partition,
            }
        )
    )
