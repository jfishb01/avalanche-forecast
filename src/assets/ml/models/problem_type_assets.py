import pandera as pa
from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    AutomationCondition,
    AssetIn,
)

from src.partitions import (
    annual_region_forecast_partitions_def,
    daily_region_forecast_partitions_def,
)
from src.assets.ml.features_and_targets.target_assets import target
from src.assets.ml.features_and_targets.avalanche_forecast_center_feature_assets import (
    avalanche_forecast_center_feature,
)
from src.assets.ml.models.asset_helpers import (
    trained_model_asset,
    model_prediction_asset,
)
from src.schemas.ml.prediction_schemas import (
    PredictionSchema,
    PredictionSchemaDagsterType,
)


PROBLEM_0_MODEL_NAME = "problem_0"
PROBLEM_1_MODEL_NAME = "problem_1"
PROBLEM_2_MODEL_NAME = "problem_2"


@asset(
    required_resource_keys={
        "duck_db_resource",
        "mlflow_resource",
        "model_deployments_config_resource",
    },
    key_prefix="training",
    group_name="ml",
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
    required_resource_keys={
        "duck_db_resource",
        "mlflow_resource",
        "model_deployments_config_resource",
    },
    key_prefix="training",
    group_name="ml",
    compute_kind="python",
    partitions_def=annual_region_forecast_partitions_def,
    deps=[
        target,
        avalanche_forecast_center_feature,
    ],
)
def problem_type_1_trained_model(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """Train a problem_1 model for an avalanche season and region and log the trained model to mlflow."""
    return trained_model_asset(context, PROBLEM_1_MODEL_NAME)


@asset(
    required_resource_keys={
        "duck_db_resource",
        "mlflow_resource",
        "model_deployments_config_resource",
    },
    key_prefix="training",
    group_name="ml",
    compute_kind="python",
    partitions_def=annual_region_forecast_partitions_def,
    deps=[
        target,
        avalanche_forecast_center_feature,
    ],
)
def problem_type_2_trained_model(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """Train a problem_2 model for an avalanche season and region and log the trained model to mlflow."""
    return trained_model_asset(context, PROBLEM_2_MODEL_NAME)


@asset(
    name="problem_type_0",
    io_manager_key="duck_db_io_manager",
    required_resource_keys={
        "duck_db_resource",
        "mlflow_resource",
        "model_deployments_config_resource",
    },
    key_prefix="prediction",
    group_name="ml",
    compute_kind="python",
    partitions_def=daily_region_forecast_partitions_def,
    metadata={
        "partition_expr": {
            "region_id": "region_id",
            "forecast_date": "forecast_date",
        },
        "schema": "forecasts",
    },
    deps=[avalanche_forecast_center_feature],
    dagster_type=PredictionSchemaDagsterType,
    automation_condition=AutomationCondition.eager(),
)
def problem_type_0_prediction(
    context: AssetExecutionContext,
) -> pa.typing.DataFrame[PredictionSchema]:
    """Generate predictions for problem_type_0 and write the outputs to a database."""
    return model_prediction_asset(context, PROBLEM_0_MODEL_NAME)


@asset(
    name="problem_type_1",
    io_manager_key="duck_db_io_manager",
    required_resource_keys={
        "duck_db_resource",
        "mlflow_resource",
        "model_deployments_config_resource",
    },
    key_prefix="prediction",
    group_name="ml",
    compute_kind="python",
    partitions_def=daily_region_forecast_partitions_def,
    metadata={
        "partition_expr": {
            "region_id": "region_id",
            "forecast_date": "forecast_date",
        },
        "schema": "forecasts",
    },
    ins={
        "problem_type_0": AssetIn(
            key=problem_type_0_prediction.key,
            dagster_type=PredictionSchemaDagsterType,
            input_manager_key="duck_db_io_manager",
        ),
    },
    deps=[avalanche_forecast_center_feature],
    dagster_type=PredictionSchemaDagsterType,
    automation_condition=AutomationCondition.eager(),
)
def problem_type_1_prediction(
    context: AssetExecutionContext,
    problem_type_0: pa.typing.DataFrame[PredictionSchema],
) -> pa.typing.DataFrame[PredictionSchema]:
    """Generate predictions for problem_type_1 and write the outputs to a database."""
    return model_prediction_asset(
        context,
        PROBLEM_1_MODEL_NAME,
        predict_params={
            "problem_type_0": problem_type_0["forecast"].values[0],
        },
    )


@asset(
    name="problem_type_2",
    io_manager_key="duck_db_io_manager",
    required_resource_keys={
        "duck_db_resource",
        "mlflow_resource",
        "model_deployments_config_resource",
    },
    key_prefix="prediction",
    group_name="ml",
    compute_kind="python",
    partitions_def=daily_region_forecast_partitions_def,
    metadata={
        "partition_expr": {
            "region_id": "region_id",
            "forecast_date": "forecast_date",
        },
        "schema": "forecasts",
    },
    ins={
        "problem_type_0": AssetIn(
            key=problem_type_0_prediction.key,
            dagster_type=PredictionSchemaDagsterType,
            input_manager_key="duck_db_io_manager",
        ),
        "problem_type_1": AssetIn(
            key=problem_type_1_prediction.key,
            dagster_type=PredictionSchemaDagsterType,
            input_manager_key="duck_db_io_manager",
        ),
    },
    deps=[avalanche_forecast_center_feature],
    dagster_type=PredictionSchemaDagsterType,
    automation_condition=AutomationCondition.eager(),
)
def problem_type_2_prediction(
    context: AssetExecutionContext,
    problem_type_0: pa.typing.DataFrame[PredictionSchema],
    problem_type_1: pa.typing.DataFrame[PredictionSchema],
) -> pa.typing.DataFrame[PredictionSchema]:
    """Generate a prediction for problem_type_2 and write the outputs to a database."""
    return model_prediction_asset(
        context,
        PROBLEM_2_MODEL_NAME,
        predict_params={
            "problem_type_0": float(problem_type_0["forecast"].values[0]),
            "problem_type_1": float(problem_type_1["forecast"].values[0]),
        },
    )
