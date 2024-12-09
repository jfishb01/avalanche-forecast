from dagster import (
    asset,
    AssetExecutionContext,
)

from src.core.ml.models.problem_0.v000 import MlModelProblem0
from src.core.ml.train import train_model
from src.partitions import (
    annual_region_forecast_partitions_def,
    avalanche_season_and_region_id_partitions_from_key,
)
from src.assets.ml.features_and_targets.target_assets import target
from src.assets.ml.features_and_targets.avalanche_forecast_center_feature_assets import (
    avalanche_forecast_center_feature,
)


@asset(
    required_resource_keys={
        "duck_db_resource",
        "mlflow_resource",
    },
    key_prefix="ml",
    group_name="model_training",
    compute_kind="python",
    partitions_def=annual_region_forecast_partitions_def,
    deps=[
        target,
        avalanche_forecast_center_feature,
    ],
)
def problem_type_0_trained_model(
    context: AssetExecutionContext,
) -> None:
    """Trains a problem_0 model for an avalanche season and region and logs the trained model to mlflow."""
    mlflow_resource = context.resources.mlflow_resource
    avalanche_season, region_id = avalanche_season_and_region_id_partitions_from_key(
        context.partition_key
    )
    model = MlModelProblem0()
    y_test, y_pred = train_model(
        model, avalanche_season, region_id, context.resources.duck_db_resource
    )
    metrics = model.metrics(y_test, y_pred)
    with mlflow_resource.start_run(model, avalanche_season, region_id):
        mlflow_resource.log_model(
            model=model,
            avalanche_season=avalanche_season,
            region_id=region_id,
            metrics=metrics,
            y_test=y_test,
            y_pred=y_pred,
        )
