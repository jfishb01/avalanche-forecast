import yaml
from typing import Dict, Any

from dagster import ConfigurableResource


class ModelDeploymentsConfigResource(ConfigurableResource):
    """Resource for loading the model deployments configuration file."""

    config_file_path: str

    def load_config(self, model_name: str) -> Dict[str, Any]:
        with open(self.config_file_path) as f:
            yaml_config = yaml.safe_load(f)
        return [el for el in yaml_config if el["model"] == model_name][0]
