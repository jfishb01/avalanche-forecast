from dagster_pandera import pandera_schema_to_dagster_type
from datetime import date

from src.schemas.schema_config import BaseMLSchema


class ForecastSchema(BaseMLSchema):
    """Pandera schema for model forecasts."""

    analysis_date: date
    forecast_date: date
    avalanche_season: str
    region_id: str
    forecast: float
    release: str
    mlflow_model_uri: str


ForecastSchemaDagsterType = pandera_schema_to_dagster_type(ForecastSchema)
