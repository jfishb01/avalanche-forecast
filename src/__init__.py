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

from src.assets.ingestion import caic_assets
from src.schedules.ingestion_schedules import caic_ingestion_schedule
from src.resources.core.file_io_managers import JSONFileIOManager
from src.resources.extraction.avalanche_information_center_resources import CAICResource

warnings.filterwarnings("ignore", category=ExperimentalWarning)


env = os.getenv("DEFINITIONS", "DEV").upper()


def env_assets(env: str) -> Sequence[AssetsDefinition]:
    return load_assets_from_modules([caic_assets])


def env_jobs(env: str) -> Sequence[JobDefinition]:
    return []


def env_schedules(env: str) -> Sequence[ScheduleDefinition]:
    return [caic_ingestion_schedule]


def env_sensors(env: str) -> Sequence[SensorDefinition]:
    return []


def env_resources(
    env: str,
) -> Dict[str, Union[ConfigurableResource, ConfigurableIOManager]]:
    if env == "DEV":
        dev_base_dir = "downloads/dev"
        return {
            "json_file_io_manager": JSONFileIOManager(
                root_path=dev_base_dir, dump_fn_kwargs={"indent": 2}
            ),
            "caic_resource": CAICResource(),
        }
    if env == "PROD":
        prod_base_dir = "downloads/prod"
        return {
            "json_file_io_manager": JSONFileIOManager(
                root_path=prod_base_dir, dump_fn_kwargs={"indent": 2}
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
