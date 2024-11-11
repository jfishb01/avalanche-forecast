import pytz
from datetime import datetime
from dagster import DailyPartitionsDefinition


colorado_region_forecast_partitions_def = DailyPartitionsDefinition(
    start_date=datetime(2023, 1, 1, tzinfo=pytz.UTC),
    timezone="UTC",
    end_offset=1,  # Ensure a partition for today exists
)
