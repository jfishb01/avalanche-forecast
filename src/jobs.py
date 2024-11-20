from dagster import define_asset_job
from src.assets.ingestion.avalanche_forecast_center_assets import (
    combined_avalanche_forecast_center_forecast,
)
from src.assets.ml.features_and_targets.target_assets import target
from src.assets.ml.features_and_targets.avalanche_forecast_center_feature_assets import (
    avalanche_forecast_center_feature,
)

# Job for aggregating forecasts from a number of different avalanche forecast centers and
# saving the forecasts to a combined DB table.
combined_avalanche_forecast_center_forecast_job = define_asset_job(
    "combined_avalanche_forecast_center_forecast",
    selection=[combined_avalanche_forecast_center_forecast],
)

target_creation_job = define_asset_job("target_creation", selection=[target])

avalanche_forecast_center_feature_creation_job = define_asset_job(
    "avalanche_forecast_center_feature_creation",
    selection=[avalanche_forecast_center_feature],
)
