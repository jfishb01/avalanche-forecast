from dagster_pandera import pandera_schema_to_dagster_type

from src.schemas.schema_config import BaseMLSchema
from src.schemas.ingestion.avalanche_forecast_center_schemas import (
    AvalancheForecastCenterForecastSchema,
)


class AvalancheForecastCenterForecastFeatureSchema(
    BaseMLSchema, AvalancheForecastCenterForecastSchema
):
    """Pandera schema for ML model features from avalanche forecast center forecasts."""

    pass


AvalancheForecastCenterForecastFeatureSchemaDagsterType = (
    pandera_schema_to_dagster_type(AvalancheForecastCenterForecastFeatureSchema)
)
