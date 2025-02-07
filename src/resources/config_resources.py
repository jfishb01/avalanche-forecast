import yaml
from typing import Dict, Any
from dagster import ConfigurableResource

from src.schemas.ml.model_config_schemas import ModelConfigSchema


class ModelConfigYAMLError(BaseException):
    pass


class ModelDeploymentsConfigResource(ConfigurableResource):
    """Resource for loading the model deployments configuration file."""

    config_file_path: str

    def load_config(self, model_name: str) -> ModelConfigSchema:
        """Get the import path for the corresponding model_name as defined in the config yaml file.

        The config yaml file must be structured as:
          model_name1: import_path1
          model_name2: import_path2
        """
        with open(self.config_file_path) as f:
            yaml_config = yaml.safe_load(f)
        try:
            return ModelConfigSchema(
                model_name=model_name, import_path=yaml_config[model_name]
            )
        except IndexError:
            raise ModelConfigYAMLError(
                f"Missing model deployment config entry for '{model_name}'"
            )
