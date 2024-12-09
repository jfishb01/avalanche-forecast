from dagster import schedule, ScheduleEvaluationContext, RunRequest

from src.utils.constants import DEFAULT_SCHEDULE_EXECUTION_STATUS
from src.assets.ingestion.avalanche_forecast_center_assets import (
    raw_caic_forecast,
    raw_nwac_forecast,
)


@schedule(
    target=raw_caic_forecast,
    cron_schedule=f"0 8 * * *",
    default_status=DEFAULT_SCHEDULE_EXECUTION_STATUS,
    description=f"Schedule to ingest daily CAIC forecasts",
)
def caic_ingestion_schedule(
    context: ScheduleEvaluationContext,
):
    forecast_date = context.scheduled_execution_time.date().isoformat()
    yield RunRequest(partition_key=forecast_date)


@schedule(
    target=raw_nwac_forecast,
    cron_schedule=f"0 8 * * *",
    default_status=DEFAULT_SCHEDULE_EXECUTION_STATUS,
    description=f"Schedule to ingest daily NWAC forecasts",
)
def nwac_ingestion_schedule(
    context: ScheduleEvaluationContext,
):
    forecast_date = context.scheduled_execution_time.date().isoformat()
    yield RunRequest(partition_key=forecast_date)
