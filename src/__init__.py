import os
import warnings
from typing import Dict, Union, Sequence
from dagster import (
    load_assets_from_modules,
    Definitions,
    ExperimentalWarning,
    ConfigurableResource,
    ConfigurableIOManager,
    AssetsDefinition,
    JobDefinition,
    ScheduleDefinition,
    SensorDefinition,
)
from dagster_duckdb import DuckDBResource

from src import jobs
from src.assets.ingestion import avalanche_forecast_center_assets
from src.assets.ml.features_and_targets import target_assets
from src.assets.ml.features_and_targets import avalanche_forecast_center_feature_assets
from src.assets.ml.models import problem_type_assets
from src.schedules import ingestion_schedules
from src.sensors.ingestion.avalanche_forecast_center_sensors import (
    raw_nwac_forecast_materialization_sensor,
)
from src.sensors.ml.features_and_targets.target_sensors import (
    combined_avalanche_forecast_center_forecast_target_materialization_sensor,
)
from src.sensors.ml.features_and_targets.avalanche_forecast_center_feature_sensors import (
    combined_avalanche_forecast_center_forecast_feature_materialization_sensor,
)
from src.resources.config_resources import ModelDeploymentsConfigResource
from src.resources.core.mlflow_resource import MlflowResource
from src.resources.core.file_io_managers import JSONFileIOManager
from src.resources.core.duck_db_io_manager import DuckDBPandasIOManager
from src.resources.extraction.avalanche_information_center_resources import (
    CAICResource,
    NWACResource,
)

warnings.filterwarnings("ignore", category=ExperimentalWarning)


env = os.getenv("DEFINITIONS", "DEV").upper()


def env_assets(env: str) -> Sequence[AssetsDefinition]:
    """Load assets according to the user environment."""
    return load_assets_from_modules(
        [
            avalanche_forecast_center_assets,
            target_assets,
            avalanche_forecast_center_feature_assets,
            problem_type_assets,
        ]
    )


def env_jobs(env: str) -> Sequence[JobDefinition]:
    """Load jobs according to the user environment."""
    return [
        jobs.combined_avalanche_forecast_center_forecast_job,
        jobs.target_creation_job,
        jobs.avalanche_forecast_center_feature_creation_job,
    ]


def env_schedules(env: str) -> Sequence[ScheduleDefinition]:
    """Load schedules according to the user environment."""
    return [
        ingestion_schedules.nwac_ingestion_schedule,
    ]


def env_sensors(env: str) -> Sequence[SensorDefinition]:
    """Load sensors according to the user environment."""
    return [
        raw_nwac_forecast_materialization_sensor,
        combined_avalanche_forecast_center_forecast_target_materialization_sensor,
        combined_avalanche_forecast_center_forecast_feature_materialization_sensor,
    ]


def env_resources(
    env: str,
) -> Dict[str, Union[ConfigurableResource, ConfigurableIOManager]]:
    """Load resources according to the user environment."""
    if env == "DEV":
        dev_base_dir = "data/dev"
        return {
            "source_data_duck_db_io_manager": DuckDBPandasIOManager(
                database=os.path.join(dev_base_dir, "source_data.duckdb")
            ),
            "duck_db_io_manager": DuckDBPandasIOManager(
                database=os.path.join(dev_base_dir, "avalanche_forecast.duckdb")
            ),
            "json_file_io_manager": JSONFileIOManager(
                root_path=dev_base_dir, dump_fn_kwargs={"indent": 2}
            ),
            "source_data_duck_db_resource": DuckDBResource(
                database=os.path.join(dev_base_dir, "source_data.duckdb")
            ),
            "duck_db_resource": DuckDBResource(
                database=os.path.join(dev_base_dir, "avalanche_forecast.duckdb")
            ),
            "mlflow_resource": MlflowResource(
                tracking_server_uri="http://mlflow-webserver:5000",
                web_browser_uri="http://localhost:5000",
            ),
            "model_deployments_config_resource": ModelDeploymentsConfigResource(
                config_file_path="src/config/model_deployments.yaml"
            ),
            "caic_resource": CAICResource(),
            "nwac_resource": NWACResource(),
        }
    if env == "PROD":
        prod_base_dir = "data/prod"
        return {
            "source_data_duck_db_io_manager": DuckDBPandasIOManager(
                database=os.path.join(prod_base_dir, "source_data.duckdb")
            ),
            "duck_db_io_manager": DuckDBPandasIOManager(
                database=os.path.join(prod_base_dir, "avalanche_forecast.duckdb")
            ),
            "json_file_io_manager": JSONFileIOManager(
                root_path=prod_base_dir, dump_fn_kwargs={"indent": 2}
            ),
            "source_data_duck_db_resource": DuckDBPandasIOManager(
                database=os.path.join(prod_base_dir, "source_data.duckdb")
            ),
            "duck_db_resource": DuckDBResource(
                database=os.path.join(prod_base_dir, "avalanche_forecast.duckdb")
            ),
            "mlflow_resource": MlflowResource(
                tracking_server_uri="http://mlflow-webserver:5000",
                web_browser_uri="http://localhost:5000",
            ),
            "model_deployments_config_resource": ModelDeploymentsConfigResource(
                config_file_path="src/config/prod_model_config.yaml"
            ),
            "caic_resource": CAICResource(),
            "nwac_resource": NWACResource(),
        }


defs = Definitions(
    assets=env_assets(env),
    jobs=env_jobs(env),
    schedules=env_schedules(env),
    sensors=env_sensors(env),
    resources=env_resources(env),
)
