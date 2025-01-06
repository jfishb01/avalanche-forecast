import pandas as pd
import pandera as pa
from datetime import timedelta
from typing import Optional
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
)

from src.core.ml.train import train_model
from src.utils.schema_helpers import conform_to_schema
from src.partitions import (
    avalanche_season_and_region_id_partitions_from_key,
    forecast_date_and_region_id_partitions_from_key,
)
from src.core.ml.models.base_ml_model import ModelFactory
from src.utils.datetime_helpers import date_to_avalanche_season
from src.schemas.ml.inference_schemas import ForecastSchema


def trained_model_asset(
    context: AssetExecutionContext, model_name: str
) -> MaterializeResult:
    """Core asset logic for training a model for an avalanche season and region and logging the model to mlflow."""
    mlflow_resource = context.resources.mlflow_resource
    avalanche_season, region_id = avalanche_season_and_region_id_partitions_from_key(
        context.partition_key
    )
    model_config = context.resources.model_deployments_config_resource.load_config(
        model_name
    )
    model = ModelFactory.create_model_instance_from_config(model_config)
    y_test, y_pred = train_model(
        model, avalanche_season, region_id, context.resources.duck_db_resource
    )
    metrics = model.metrics(y_test, y_pred)
    with mlflow_resource.start_run(model, avalanche_season, region_id) as run:
        run_uri = mlflow_resource.run_uri(run)
        model_uri = mlflow_resource.log_model(
            model=model,
            avalanche_season=avalanche_season,
            region_id=region_id,
            metrics=metrics,
            y_test=y_test,
            y_pred=y_pred,
        )
    return MaterializeResult(
        metadata={
            "mlflow_run_uri": MetadataValue.url(run_uri),
            "mlflow_model_uri": MetadataValue.url(model_uri),
            "release": model.release,
        }
        | metrics
    )


def model_inference_asset(
    context: AssetExecutionContext,
    model_name: str,
    predict_params: Optional[dict] = None,
) -> pa.typing.DataFrame[ForecastSchema]:
    """Core asset logic for performing model inference for an avalanche season and region."""
    mlflow_resource = context.resources.mlflow_resource
    forecast_date, region_id = forecast_date_and_region_id_partitions_from_key(
        context.partition_key
    )
    avalanche_season = date_to_avalanche_season(forecast_date)
    model_config = context.resources.model_deployments_config_resource.load_config(
        model_name
    )
    model = mlflow_resource.load_model(model_config, avalanche_season, region_id)
    features = model.get_features(
        forecast_date, region_id, context.resources.duck_db_resource
    )
    forecast = model.predict(None, features.values, predict_params)
    mlflow_model_uri = mlflow_resource.get_deployed_model_uri(
        model, avalanche_season, region_id
    )
    df = pd.DataFrame(
        dict(
            run_id=context.run_id,
            run_key=context.partition_key,
            analysis_date=forecast_date - timedelta(days=1),
            forecast_date=forecast_date,
            avalanche_season=date_to_avalanche_season(forecast_date),
            region_id=region_id,
            forecast=forecast,
            release=model.release,
            mlflow_model_uri=mlflow_model_uri,
        )
    )
    context.add_output_metadata(
        {
            "mlflow_model_uri": mlflow_resource.get_deployed_model_uri(
                model, avalanche_season, region_id
            ),
            "release": model.release,
            "forecast": float(forecast[0]),
        }
    )
    return conform_to_schema(df, ForecastSchema)
