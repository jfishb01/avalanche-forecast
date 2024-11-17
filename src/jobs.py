from dagster import define_asset_job
from src.assets.ingestion.avalanche_forecast_center_assets import (
    avalanche_forecast_center_forecast,
)


# Job for aggregating forecasts from a number of different avalanche forecast centers and
# saving the forecasts to a combined DB table.
avalanche_forecast_center_forecast_job = define_asset_job(
    "avalanche_forecast_center_forecast", selection=[avalanche_forecast_center_forecast]
)
