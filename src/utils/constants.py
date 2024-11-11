import os
from dagster import DefaultScheduleStatus, DefaultSensorStatus


DEFAULT_SCHEDULE_EXECUTION_STATUS = getattr(
    DefaultScheduleStatus, os.getenv("DEFAULT_EXECUTION_STATUS", "STOPPED")
)
DEFAULT_SENSOR_EXECUTION_STATUS = getattr(
    DefaultSensorStatus, os.getenv("DEFAULT_EXECUTION_STATUS", "STOPPED")
)
