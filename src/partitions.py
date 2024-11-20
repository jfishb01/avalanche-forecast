import pytz
from typing import Tuple
from datetime import datetime, date
from dagster import (
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    MultiPartitionKey,
)


def forecast_date_and_area_partitions_from_key(
    partition_key: MultiPartitionKey,
) -> Tuple[date, str]:
    """Split the daily_area_forecast_partitions_def into its forecast_date and forecast_area components."""
    forecast_date = datetime.fromisoformat(
        partition_key.keys_by_dimension["forecast_date"]
    ).date()
    forecast_area = partition_key.keys_by_dimension["forecast_area"]
    return forecast_date, forecast_area


def forecast_center_and_area_id_from_forecast_area(
    forecast_area: str,
) -> Tuple[str, str]:
    """Split a forecast_area string into components of avalanche forecast center and area ID."""
    forecast_center, area_id = forecast_area.split(".", 1)
    return forecast_center, area_id


# Static partitions definition for all area IDs belonging to each avalanche forecast center
forecast_area_partitions_def = StaticPartitionsDefinition(
    [
        "CAIC.efc9235da6ac8cf1c6873ceed63568e0d51371ada5a31dd0ffa4e7e35908f376",
        "CAIC.4a8152341231e8038706c6f855c56d38679433bd8b401573c4eed9d7d76cae71",
        "CAIC.128dd258177cb3cd83a5e91f8d5342b0ad917a0a2759fb600818c9f7aa01dc56",
        "CAIC.17cccd88f3dd4a93834f348c57aa75c0379ef1d9fa364ac3aa491442e9e23970",
        "CAIC.c65550169c6b48400b2e15153beb523f3acf325607b94b10a0d6beb2aa1ab3e0",
        "CAIC.5a85f8a5c5c6da01b4c0504b0a4012d28bcdf27e8aa95f4c974e92dad834c8e8",
        "CAIC.61c52bf2655339bedb5d0923d3acbc748a6141dbf732ec5742340d2293827c58",
        "CAIC.918583ebeb62fcad25d1f80c9b3fade2f32e4de5e85f4d0159244027b013394e",
    ]
)

# Daily partition definition for each date that will be forecasted
forecast_date_partitions_def = DailyPartitionsDefinition(
    start_date=datetime(2020, 1, 1, tzinfo=pytz.UTC),
    timezone="UTC",
    end_offset=1,  # Ensure a partition for today exists
)

# Multipartition on forecast date by area to be used by all models and features
daily_area_forecast_partitions_def = MultiPartitionsDefinition(
    {
        "forecast_date": forecast_date_partitions_def,
        "forecast_area": forecast_area_partitions_def,
    }
)
