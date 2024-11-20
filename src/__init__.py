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
from dagster_duckdb_pandas import DuckDBPandasIOManager

from src import jobs
from src.assets.ingestion import avalanche_forecast_center_assets
from src.assets.ml.features_and_targets import target_assets
from src.assets.ml.features_and_targets import avalanche_forecast_center_feature_assets
from src.schedules.ingestion_schedules import caic_ingestion_schedule
from src.sensors.ingestion.avalanche_forecast_center_sensors import (
    raw_caic_forecast_materialization_sensor,
)
from src.sensors.ml.features_and_targets.target_sensors import (
    combined_avalanche_forecast_center_forecast_target_materialization_sensor,
)
from src.sensors.ml.features_and_targets.avalanche_forecast_center_feature_sensors import (
    combined_avalanche_forecast_center_forecast_feature_materialization_sensor,
)
from src.resources.core.file_io_managers import JSONFileIOManager
from src.resources.extraction.avalanche_information_center_resources import CAICResource

warnings.filterwarnings("ignore", category=ExperimentalWarning)


env = os.getenv("DEFINITIONS", "DEV").upper()


def env_assets(env: str) -> Sequence[AssetsDefinition]:
    """Load assets according to the user environment."""
    return load_assets_from_modules(
        [
            avalanche_forecast_center_assets,
            target_assets,
            avalanche_forecast_center_feature_assets,
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
    return [caic_ingestion_schedule]


def env_sensors(env: str) -> Sequence[SensorDefinition]:
    """Load sensors according to the user environment."""
    return [
        raw_caic_forecast_materialization_sensor,
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
            "duck_db_io_manager": DuckDBPandasIOManager(
                database=os.path.join(dev_base_dir, "avalanche_forecast.duckdb")
            ),
            "json_file_io_manager": JSONFileIOManager(
                root_path=dev_base_dir, dump_fn_kwargs={"indent": 2}
            ),
            "duck_db_resource": DuckDBResource(
                database=os.path.join(dev_base_dir, "avalanche_forecast.duckdb")
            ),
            "caic_resource": CAICResource(),
        }
    if env == "PROD":
        prod_base_dir = "data/prod"
        return {
            "duck_db_io_manager": DuckDBPandasIOManager(
                database=os.path.join(prod_base_dir, "avalanche_forecast.duckdb")
            ),
            "json_file_io_manager": JSONFileIOManager(
                root_path=prod_base_dir, dump_fn_kwargs={"indent": 2}
            ),
            "duck_db_resource": DuckDBResource(
                database=os.path.join(prod_base_dir, "avalanche_forecast.duckdb")
            ),
            "caic_resource": CAICResource(),
        }


defs = Definitions(
    assets=env_assets(env),
    jobs=env_jobs(env),
    schedules=env_schedules(env),
    sensors=env_sensors(env),
    resources=env_resources(env),
)
