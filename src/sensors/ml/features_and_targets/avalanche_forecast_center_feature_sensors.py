from dagster import (
    RunRequest,
    EventLogEntry,
    SensorEvaluationContext,
    asset_sensor,
    MultiPartitionKey,
)

from src.jobs import avalanche_forecast_center_feature_creation_job
from src.partitions import region_id_partitions_def
from src.assets.ingestion.avalanche_forecast_center_assets import (
    combined_avalanche_forecast_center_forecast,
)


@asset_sensor(
    asset_key=combined_avalanche_forecast_center_forecast.key,
    job=avalanche_forecast_center_feature_creation_job,
)
def combined_avalanche_forecast_center_forecast_feature_materialization_sensor(
    context: SensorEvaluationContext, asset_event: EventLogEntry
) -> RunRequest:
    """Listen for materializations of combined_avalanche_forecast_center_forecast and trigger feature creation."""
    assert (
        asset_event.dagster_event
        and asset_event.dagster_event.asset_key
        and asset_event.dagster_event.partition
    )

    forecast_date = asset_event.dagster_event.partition.keys_by_dimension[
        "distribution_date"
    ]
    forecast_center = asset_event.dagster_event.partition.keys_by_dimension[
        "forecast_center"
    ]
    regions_to_process = [
        region_id
        for region_id in region_id_partitions_def.get_partition_keys()
        if region_id.startswith(f"{forecast_center}.")
    ]
    for region_id in regions_to_process:
        yield RunRequest(
            partition_key=MultiPartitionKey(
                {"forecast_date": forecast_date, "region_id": region_id}
            )
        )
