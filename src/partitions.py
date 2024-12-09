import pytz
import pandas as pd
from typing import Tuple, List, Sequence
from datetime import datetime, date
from dagster import (
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    MultiPartitionKey,
)

from src.utils.datetime_helpers import date_to_avalanche_season


def forecast_date_and_region_id_partitions_from_key(
    partition_key: MultiPartitionKey,
) -> Tuple[date, str]:
    """Split the daily_region_forecast_partitions_def into its forecast_date and region_id components."""
    forecast_date = datetime.fromisoformat(
        partition_key.keys_by_dimension["forecast_date"]
    ).date()
    region_id = partition_key.keys_by_dimension["region_id"]
    return forecast_date, region_id


def forecast_date_and_region_id_partitions_from_key_list(
    partition_keys: Sequence[str],
) -> Tuple[List[date], List[str]]:
    """Apply forecast_date_and_region_id_partitions_from_key to a list of partition keys."""
    forecast_dates, region_ids = zip(
        *[
            forecast_date_and_region_id_partitions_from_key(partition_key)
            for partition_key in partition_keys
        ]
    )
    return list(set(forecast_dates)), list(set(region_ids))


def avalanche_season_and_region_id_partitions_from_key(
    partition_key: MultiPartitionKey,
) -> Tuple[str, str]:
    """Split the annual_region_forecast_partitions_def into its avalanche_season and region_id components."""
    avalanche_season = partition_key.keys_by_dimension["avalanche_season"]
    region_id = partition_key.keys_by_dimension["region_id"]
    return avalanche_season, region_id


# Static partitions definition for all area IDs belonging to each avalanche forecast center
nwac_area_ids = [
    "1128",  # Olympics,
    "1129",  # West Slopes North
    "1130",  # West Slopes Central
    "1131",  # West Slopes South
    "1132",  # Stevens Pass
    "1136",  # Snoqualmie Pass
    "1137",  # East Slopes North
    "1138",  # East Slopes Central
    "1139",  # East Slopes South
    "1140",  # Mt Hood
]
region_id_partitions_def = StaticPartitionsDefinition(
    [f"NWAC.{area_id}" for area_id in nwac_area_ids]
)

# Static partitions definition for historical tracked avalanche seasons up to the current season
avalanche_season_partitions_def = StaticPartitionsDefinition(
    sorted(
        set(
            [
                date_to_avalanche_season(dt.date(), use_upcoming_when_unmapped=True)
                for dt in pd.date_range(date(2020, 1, 1), date.today()).to_pydatetime()
            ]
        )
    )
)

# Daily partition definition for each date that will be forecasted
forecast_date_partitions_def = DailyPartitionsDefinition(
    start_date=datetime(2020, 1, 1, tzinfo=pytz.UTC),
    timezone="UTC",
    end_offset=1,  # Ensure a partition for today exists
)

# Multipartition on forecast date by region to be used by all models and features
daily_region_forecast_partitions_def = MultiPartitionsDefinition(
    {
        "forecast_date": forecast_date_partitions_def,
        "region_id": region_id_partitions_def,
    }
)

# Multipartition on avalanche season by region to be used for annual model training
annual_region_forecast_partitions_def = MultiPartitionsDefinition(
    {
        "avalanche_season": avalanche_season_partitions_def,
        "region_id": region_id_partitions_def,
    }
)
