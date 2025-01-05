import os
import mlflow
import tempfile
import numpy as np
import pandas as pd
from typing import Dict, Any, Optional
from dagster import ConfigurableResource
from contextlib import contextmanager
from mlflow import MlflowClient
from mlflow import ActiveRun

from src.core.ml.models.base_ml_model import BaseMLModel, ModelFactory


class MlflowResource(ConfigurableResource):
    """Resource for interacting with MLflow."""

    tracking_server_uri: str
    web_browser_uri: Optional[str] = None
    _client_singleton: Optional[MlflowClient] = None

    def client(self):
        """Get a tracking server mlflow client."""
        if self._client_singleton is None:
            self._client_singleton = MlflowClient(self.tracking_server_uri)
        return self._client_singleton

    @contextmanager
    def start_run(
        self, model: BaseMLModel, avalanche_season: str, region_id: str, **kwargs
    ) -> ActiveRun:
        """Context manager for starting an mlflow experiment run.

        Creates a run for model, region, avalanche season pair. If the experiment does not yet exist,
        a new one will be created.
        """
        mlflow.set_tracking_uri(self.tracking_server_uri)
        tags = {
            "release": model.release,
            "region_id": region_id,
            "avalanche_season": avalanche_season,
        }
        if "tags" in kwargs:
            tags.update(kwargs["tags"])
            kwargs.pop("tags")

        experiment = mlflow.set_experiment(experiment_name=model.name)
        with mlflow.start_run(
            experiment_id=experiment.experiment_id, tags=tags, **kwargs
        ) as run:
            yield run

    def run_uri(self, run: ActiveRun) -> str:
        """Get a browser accessible run URI for the active run."""
        domain = self.web_browser_uri or self.tracking_server_uri
        return f"{domain}/#/experiments/{run.info.experiment_id}/runs/{run.info.run_id}"

    def log_model(
        self,
        model: BaseMLModel,
        avalanche_season: str,
        region_id: str,
        metrics: Dict[str, float],
        y_test: np.array,
        y_pred: np.array,
    ) -> str:
        """Log the model to the active run along with its performance metrics.

        If not within the context of a run, a new run will be created. Returns the mlflow URI for the model that
        was logged.
        """
        logged_model = model.log(avalanche_season, region_id)
        mlflow.log_params(model.parameters | model.features)
        mlflow.log_metrics(metrics)
        with tempfile.TemporaryDirectory() as tmp:
            predictions_file = os.path.join(tmp, "predictions.csv")
            pd.DataFrame({"y_test": y_test, "y_pred": y_pred}).to_csv(
                predictions_file, index=False
            )
            mlflow.log_artifact(predictions_file)

        registered_model_name = logged_model.model_uri.split("/")[-1]
        version = str(logged_model.registered_model_version)
        self.client().set_model_version_tag(
            registered_model_name,
            version,
            "release",
            f"{model.release}",
        )
        self.client().set_registered_model_alias(
            registered_model_name,
            f"{model.mlflow_deployment_alias}",
            version
        )
        domain = self.web_browser_uri or self.tracking_server_uri
        return f"{domain}/#/models/{registered_model_name}/versions/{version}"

    def load_model(self, model_config: Dict[str, Any], avalanche_season: str, region_id: str) -> BaseMLModel:
        """Load a deployed model from mlflow for the corresponding avalanche season and region ID."""
        mlflow.set_tracking_uri(self.tracking_server_uri)
        model_instance = ModelFactory.create_model_instance_from_config(model_config)
        model_instance.load(avalanche_season, region_id)
        return model_instance

    def get_deployed_model_uri(self, model: BaseMLModel, avalanche_season: str, region_id: str) -> str:
        """Get a browser accessible URI for the deployed model correspong to the avalanche season and region ID."""
        registered_model_name = model.id(avalanche_season, region_id)
        version = self.client().get_model_version_by_alias(
            model.id(avalanche_season, region_id), model.mlflow_deployment_alias
        ).version
        domain = self.web_browser_uri or self.tracking_server_uri
        return f"{domain}/#/models/{registered_model_name}/versions/{version}"
