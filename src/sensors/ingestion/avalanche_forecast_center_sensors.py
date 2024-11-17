from dagster import (
    RunRequest,
    EventLogEntry,
    RunConfig,
    SensorEvaluationContext,
    asset_sensor,
)

from src.jobs import avalanche_forecast_center_forecast_job
from src.assets.ingestion.avalanche_forecast_center_assets import (
    raw_caic_forecast,
    avalanche_forecast_center_forecast,
)


@asset_sensor(
    asset_key=raw_caic_forecast.key, job=avalanche_forecast_center_forecast_job
)
def extracted_avalanche_forecast_center_forecast_sensor(
    context: SensorEvaluationContext, asset_event: EventLogEntry
) -> RunRequest:
    """Listen for materialization events of extracted CAIC forecasts and add them to the combined forecast table"""
    assert (
        asset_event.dagster_event
        and asset_event.dagster_event.asset_key
        and asset_event.dagster_event.partition
    )
    yield RunRequest(
        partition_key=f"{asset_event.dagster_event.partition}|CAIC",
        run_config=RunConfig(
            ops={avalanche_forecast_center_forecast.key.to_python_identifier(): {}}
        ),
    )
