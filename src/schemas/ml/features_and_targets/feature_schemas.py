from dagster_pandera import pandera_schema_to_dagster_type

from src.schemas.schema_config import BaseMLSchema
from src.schemas.ingestion.avalanche_forecast_center_schemas import (
    AvalancheForecastCenterForecastSchema,
)


class AvalancheForecastCenterForecastFeatureSchema(
    BaseMLSchema, AvalancheForecastCenterForecastSchema
):
    """Pandera schema for ML model features from avalanche forecast center forecasts."""
    aspect_sin_alp_0: float
    aspect_cos_alp_0: float
    aspect_range_alp_0: float
    aspect_sin_tln_0: float
    aspect_cos_tln_0: float
    aspect_range_tln_0: float
    aspect_sin_btl_0: float
    aspect_cos_btl_0: float
    aspect_range_btl_0: float
    aspect_sin_alp_1: float
    aspect_cos_alp_1: float
    aspect_range_alp_1: float
    aspect_sin_tln_1: float
    aspect_cos_tln_1: float
    aspect_range_tln_1: float
    aspect_sin_btl_1: float
    aspect_cos_btl_1: float
    aspect_range_btl_1: float
    aspect_sin_alp_2: float
    aspect_cos_alp_2: float
    aspect_range_alp_2: float
    aspect_sin_tln_2: float
    aspect_cos_tln_2: float
    aspect_range_tln_2: float
    aspect_sin_btl_2: float
    aspect_cos_btl_2: float
    aspect_range_btl_2: float


AvalancheForecastCenterForecastFeatureSchemaDagsterType = (
    pandera_schema_to_dagster_type(AvalancheForecastCenterForecastFeatureSchema)
)
