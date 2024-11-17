import pytz
from datetime import datetime
from dagster import (
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)


avalanche_forecast_center_partitions_def = StaticPartitionsDefinition(["CAIC", "NWAC"])


forecast_date_partitions_def = DailyPartitionsDefinition(
    start_date=datetime(2020, 1, 1, tzinfo=pytz.UTC),
    timezone="UTC",
    end_offset=1,  # Ensure a partition for today exists
)


distribution_date_partitions_def = DailyPartitionsDefinition(
    start_date=datetime(2020, 1, 1, tzinfo=pytz.UTC),
    timezone="UTC",
    end_offset=1,  # Ensure a partition for today exists
)


forecast_area_partitions_def = DynamicPartitionsDefinition(name="forecast_area")


daily_avalanche_forecast_center_forecast_partitions_def = MultiPartitionsDefinition(
    {
        "forecast_center": avalanche_forecast_center_partitions_def,
        "distribution_date": distribution_date_partitions_def,
    }
)


daily_area_forecast_partitions_def = MultiPartitionsDefinition(
    {
        "forecast_area": forecast_area_partitions_def,
        "forecast_date": forecast_date_partitions_def,
    }
)
