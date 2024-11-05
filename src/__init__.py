import os
import warnings
from dagster import (
    load_assets_from_modules,
    Definitions,
    ExperimentalWarning,
)

from src.definitions import dev_definitions, prod_definitions

warnings.filterwarnings("ignore", category=ExperimentalWarning)

all_assets = load_assets_from_modules([])

env_resources = {
    "DEV": dev_definitions.get_resources,
    "PROD": prod_definitions.get_resources,
}
env_definitions = os.getenv("DEFINITIONS").upper()


defs = Definitions(
    assets=all_assets,
    jobs=[],
    resources=env_resources[env_definitions](),
    sensors=[],
    schedules=[],
)
