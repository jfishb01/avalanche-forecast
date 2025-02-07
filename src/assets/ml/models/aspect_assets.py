import pandera as pa
from dagster import (
    asset,
    AssetsDefinition,
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
from src.assets.ml.models import problem_type_assets
from src.assets.ml.models.asset_helpers import (
    trained_model_asset,
    model_prediction_asset,
)
from src.schemas.ml.prediction_schemas import (
    PredictionSchema,
    PredictionSchemaDagsterType,
)


def _aspect_component_trained_model_asset_factory(
    aspect_component: str, elevation: str, problem_number: int
) -> AssetsDefinition:
    model_name = f"aspect_{aspect_component}_{elevation}_{problem_number}"

    @asset(
        name=f"{model_name}_trained_model",
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
    def aspect_component_trained_model_asset(
        context: AssetExecutionContext,
    ) -> MaterializeResult:
        f"""Train a {model_name} model for an avalanche season and region and log the trained model to mlflow."""
        return trained_model_asset(context, model_name)

    return aspect_component_trained_model_asset


def _aspect_component_prediction_asset_factory(
    aspect_component: str, elevation: str, problem_number: int
) -> AssetsDefinition:
    model_name = f"aspect_{aspect_component}_{elevation}_{problem_number}"

    @asset(
        name=model_name,
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
            f"problem_type_{problem_number}": AssetIn(
                key=getattr(
                    problem_type_assets, f"problem_type_{problem_number}_prediction"
                ).key,
                dagster_type=PredictionSchemaDagsterType,
                input_manager_key="duck_db_io_manager",
            ),
        },
        deps=[avalanche_forecast_center_feature],
        dagster_type=PredictionSchemaDagsterType,
        automation_condition=AutomationCondition.eager(),
    )
    def aspect_component_prediction_asset(
        context: AssetExecutionContext, **kwargs
    ) -> pa.typing.DataFrame[PredictionSchema]:
        f"""Generate predictions for {model_name} and write the outputs to a database."""
        return model_prediction_asset(
            context, model_name, kwargs[f"problem_type_{problem_number}"]
        )

    return aspect_component_prediction_asset


aspect_component_trained_model_assets = [
    _aspect_component_trained_model_asset_factory(
        aspect_component=component,
        elevation=elevation,
        problem_number=problem_number,
    )
    for component in ("sin", "cos", "range")
    for elevation in ("alp", "tln", "btl")
    for problem_number in range(2)
]


aspect_component_prediction_assets = [
    _aspect_component_prediction_asset_factory(
        aspect_component=component,
        elevation=elevation,
        problem_number=problem_number,
    )
    for component in ("sin", "cos", "range")
    for elevation in ("alp", "tln", "btl")
    for problem_number in range(2)
]
