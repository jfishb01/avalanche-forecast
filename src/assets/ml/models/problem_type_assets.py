import pandera as pa
from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    AutomationCondition,
)

from src.partitions import (
    annual_region_forecast_partitions_def,
    daily_region_forecast_partitions_def,
)
from src.assets.ml.features_and_targets.target_assets import target
from src.assets.ml.features_and_targets.avalanche_forecast_center_feature_assets import (
    avalanche_forecast_center_feature,
)
from src.assets.ml.models.asset_helpers import trained_model_asset, model_inference_asset
from src.schemas.ml.inference_schemas import ForecastSchema, ForecastSchemaDagsterType


PROBLEM_0_MODEL_NAME = "problem_0"

@asset(
    required_resource_keys={
        "duck_db_resource",
        "mlflow_resource",
        "model_deployments_config_resource",
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
) -> MaterializeResult:
    """Train a problem_0 model for an avalanche season and region and log the trained model to mlflow."""
    return trained_model_asset(context, PROBLEM_0_MODEL_NAME)


@asset(
    name="problem_type_0",
    io_manager_key="duck_db_io_manager",
    required_resource_keys={
        "duck_db_resource",
        "mlflow_resource",
        "model_deployments_config_resource",
    },
    key_prefix="ml",
    group_name="model_training",
    compute_kind="python",
    partitions_def=daily_region_forecast_partitions_def,
    metadata={
        "partition_expr": {
            "region_id": "region_id",
            "forecast_date": "forecast_date",
        },
        "schema": "features",
    },
    dagster_type=ForecastSchemaDagsterType,
    automation_condition=AutomationCondition.eager(),
)
def problem_type_0_inference(
    context: AssetExecutionContext,
) -> pa.typing.DataFrame[ForecastSchema]:
    """Run inference for problem_type_0 and write the outputs to a database."""
    return model_inference_asset(context, PROBLEM_0_MODEL_NAME)
