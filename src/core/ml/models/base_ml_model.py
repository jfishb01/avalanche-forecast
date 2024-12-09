import os
import inspect
import numpy as np
import pandas as pd
from functools import reduce
from typing import Dict, List, Any
from dagster_duckdb import DuckDBResource
from mlflow.models import model, set_model
from mlflow.pyfunc import PythonModel, log_model

from src.utils.datetime_helpers import get_avalanche_season_date_bounds


class BaseMLModel(PythonModel):
    def __init__(
        self,
        model: object,
        target: str,
        features: Dict[str, List[str]],
        parameters: Dict[str, Any] = None,
    ):
        self.model = model
        self.target = target
        self.features = features
        self.parameters = parameters
        self.signature = None

    @property
    def version(self) -> str:
        """Model version obtained from the name of the model file."""
        # Get the path of the model class
        python_file = inspect.getfile(type(self))
        # Extract the file from the path and remove the ".py" extension
        return os.path.basename(python_file).replace(".py", "")

    @property
    def id(self) -> str:
        """Unique id generated from the target variable and the model version."""
        return f"{self.target}_{self.version}"

    @property
    def name(self) -> str:
        """The name of the model generated from the target variable."""
        return self.target

    def fit(self, X_train: np.array, y_train: np.array) -> None:
        """Fit the model to the training set."""
        self.model.fit(X_train, y_train)

    def predict(self, model_input: np.array) -> np.array:
        """Generate a prediction using the model inputs."""
        return self.model.predict(model_input)

    def log(self, avalanche_season: str, region_id: str) -> model.ModelInfo:
        """Log the model to MLflow for a run. Assumes that a run is currently active."""
        set_model(self)
        model_name = f"{self.name}.{region_id}.{avalanche_season}".replace("/", "-")
        model_info = log_model(
            python_model=inspect.getfile(type(self)),
            artifact_path=model_name,
            signature=self.signature,
            registered_model_name=model_name,
        )
        return model_info

    def metrics(self, y_true: np.array, y_pred: np.array, **kwargs) -> Dict[str, float]:
        raise NotImplementedError

    def get_targets(
        self, avalanche_season: str, region_id: str, db_resource: DuckDBResource
    ) -> pd.DataFrame:
        season_start, _ = get_avalanche_season_date_bounds(avalanche_season)
        with db_resource.get_connection() as conn:
            targets = conn.execute(
                f"""
                SELECT
                    forecast_date,
                    {self.target}
                FROM targets.target
                WHERE region_id = $region_id
                    AND forecast_date::DATE < $season_start
--                 ORDER BY forecast_date
                """,
                {"region_id": region_id, "season_start": season_start},
            ).df()
        targets["forecast_date"] = pd.to_datetime(targets["forecast_date"]).dt.date
        return targets

    def get_features(
        self, avalanche_season: str, region_id: str, db_resource: DuckDBResource
    ) -> pd.DataFrame:
        season_start, _ = get_avalanche_season_date_bounds(avalanche_season)
        feature_dfs = []
        for feature_table, feature_columns in self.features.items():
            with db_resource.get_connection() as conn:
                features = conn.execute(
                    f"""
                    SELECT
                        forecast_date,
                        {', '.join(feature_columns)}
                    FROM features.{feature_table}
                    WHERE region_id = $region_id
                        AND forecast_date::DATE < $season_start
                    ORDER BY forecast_date
                    """,
                    {
                        "region_id": region_id,
                        "season_start": season_start,
                    },
                ).df()
                features["forecast_date"] = pd.to_datetime(
                    features["forecast_date"]
                ).dt.date
                feature_dfs.append(features)
        return reduce(
            lambda x, y: pd.merge(x, y, on="forecast_date", hpw="inner"), feature_dfs
        )
