import os
import mlflow
import tempfile
import numpy as np
import pandas as pd
from typing import Dict
from dagster import ConfigurableResource
from contextlib import contextmanager

from src.core.ml.models.base_ml_model import BaseMLModel


class MlflowResource(ConfigurableResource):
    """Resource for interacting with MLflow."""

    tracking_server_uri: str

    @contextmanager
    def start_run(
        self, model: BaseMLModel, avalanche_season: str, region_id: str, **kwargs
    ):
        """Context manager for starting an mlflow experiment run.

        Creates a run for model, region, avalanche season pair. If the experiment does not yet exist,
        a new one will be created.
        """
        mlflow.set_tracking_uri(self.tracking_server_uri)
        tags = {
            "version": model.version,
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

    def log_model(
        self,
        model: BaseMLModel,
        avalanche_season: str,
        region_id: str,
        metrics: Dict[str, float],
        y_test: np.array,
        y_pred: np.array,
    ) -> None:
        """Log the model to the active run along with its performance metrics.

        If not within the context of a run, a new run will be created."""
        model.log(avalanche_season, region_id)
        mlflow.log_params(model.parameters)
        mlflow.log_metrics(metrics)
        with tempfile.TemporaryDirectory() as tmp:
            predictions_file = os.path.join(tmp, "predictions.csv")
            pd.DataFrame({"y_test": y_test, "y_pred": y_pred}).to_csv(
                predictions_file, index=False
            )
            mlflow.log_artifact(predictions_file)
