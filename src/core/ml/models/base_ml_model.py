import io
import os
import inspect
import importlib
import numpy as np
import pandas as pd
from datetime import date
from functools import reduce, partial
from typing import Dict, List, Any, Optional
from dagster_duckdb import DuckDBResource
from mlflow.models import model, set_model
from mlflow.pyfunc import PythonModel, PythonModelContext, log_model, load_model
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    mean_absolute_error,
    mean_absolute_percentage_error,
    root_mean_squared_error,
    r2_score,
    ConfusionMatrixDisplay,
)


from src.utils.datetime_helpers import get_avalanche_season_date_bounds
from src.schemas.ml.model_config_schemas import ModelConfigSchema


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
        self.mlflow_browser_uri = None

    @property
    def release(self) -> str:
        """Model release version obtained from the name of the model file."""
        # Get the path of the model class
        python_file = inspect.getfile(type(self))
        # Extract the file from the path and remove the ".py" extension
        return os.path.basename(python_file).replace(".py", "")

    @property
    def name(self) -> str:
        """The name of the model generated from the target variable."""
        return self.target

    def id(self, region_id: str, avalanche_season: str) -> str:
        """Unique id generated from the model name, region id, and avalanche season."""
        return f"{self.name}.{region_id}.{avalanche_season}".replace("/", "-")

    @property
    def mlflow_deployment_alias(self):
        """Get the model alias for the deployed model on Mlflow."""
        return f"{self.release}-deployment"

    def fit(self, X_train: np.array, y_train: np.array) -> None:
        """Fit the model to the training set."""
        self.model.fit(X_train, y_train)

    def predict(
        self,
        context: Optional[PythonModelContext],
        model_input: np.array,
        params: Optional[dict] = None,
    ) -> np.array:
        """Generate a prediction using the model inputs. Must shadow the signature of base PythonModel class.

        Args:
            context: Optional PythonModelContext instance containing artifacts that the internal model can
                use to generate a prediction.
            model_input: A pyfunc-compatible input for the model to evaluate.
            params: Additional parameters to pass to the model for prediction.
        """
        predict_fn = partial(self.model.predict, model_input.astype(np.float32))
        if params is not None:
            return predict_fn(params)
        return predict_fn()

    def log(self, avalanche_season: str, region_id: str) -> model.ModelInfo:
        """Log the model to MLflow for a run. Assumes that a run is currently active."""
        set_model(self)
        model_info = log_model(
            python_model=self,
            artifact_path=self.id(avalanche_season, region_id),
            signature=self.signature,
            registered_model_name=self.id(avalanche_season, region_id),
        )
        return model_info

    def load(self, avalanche_season: str, region_id: str) -> None:
        """Load the model from MLflow."""
        set_model(self)
        self.model = load_model(
            f"models:/{self.id(avalanche_season, region_id)}@{self.mlflow_deployment_alias}"
        )

    def metrics(self, y_true: np.array, y_pred: np.array, **kwargs) -> Dict[str, float]:
        """Dictionary of metrics to be displayed with Dagster materializations and MLFlow experiments."""
        return {}

    def artifacts(
        self, y_true: np.array, y_pred: np.array, **kwargs
    ) -> Dict[str, io.BytesIO]:
        """Dictionary of artifacts to be displayed with Dagster materializations and MLFlow experiments.

        Artifact values are assumed to be bytes of png images to be displayed.
        """
        return {}

    def get_targets_train(
        self, avalanche_season: str, region_id: str, db_resource: DuckDBResource
    ) -> pd.DataFrame:
        """Get targets for model training."""
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
                ORDER BY forecast_date
                """,
                {"region_id": region_id, "season_start": season_start},
            ).df()
        targets["forecast_date"] = pd.to_datetime(targets["forecast_date"]).dt.date
        return targets

    def get_features_train(
        self, avalanche_season: str, region_id: str, db_resource: DuckDBResource
    ) -> pd.DataFrame:
        """Get features for model training."""
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

    def get_features(
        self, forecast_date: date, region_id: str, db_resource: DuckDBResource
    ) -> pd.DataFrame:
        """Get features for model prediction."""
        feature_dfs = []
        for feature_table, feature_columns in self.features.items():
            with db_resource.get_connection() as conn:
                features = conn.execute(
                    f"""
                    SELECT
                        {', '.join(feature_columns)}
                    FROM features.{feature_table}
                    WHERE region_id = $region_id
                        AND forecast_date::DATE = $forecast_date
                    ORDER BY forecast_date
                    """,
                    {
                        "region_id": region_id,
                        "forecast_date": forecast_date,
                    },
                ).df()
                feature_dfs.append(features)
        return reduce(lambda x, y: pd.merge(x, y, hpw="inner"), feature_dfs)


class BaseMLModelClassification(BaseMLModel):
    def __init__(
        self,
        model: object,
        target: str,
        features: Dict[str, List[str]],
        classes: List[Any],
        parameters: Dict[str, Any] = None,
    ):
        super().__init__(
            target=target,
            features=features,
            model=model,
            parameters=parameters,
        )
        self.classes = classes

    def metrics(self, y_true: np.array, y_pred: np.array, **kwargs) -> Dict[str, float]:
        return {
            "accuracy_score": accuracy_score(y_true, y_pred),
            "precision_score": precision_score(y_true, y_pred, average="macro"),
            "recall_score": recall_score(y_true, y_pred, average="macro"),
            "f1_score": f1_score(y_true, y_pred, average="macro"),
        }

    def artifacts(
        self, y_true: np.array, y_pred: np.array, **kwargs
    ) -> Dict[str, io.BytesIO]:
        confusion_matrix = ConfusionMatrixDisplay.from_predictions(
            y_true, y_pred, labels=self.classes
        ).plot()
        buf = io.BytesIO()
        confusion_matrix.plot().figure_.savefig(buf, format="png")
        buf.seek(0)
        return {"confusion_matrix": buf}


class BaseMLModelRegression(BaseMLModel):
    def __init__(
        self,
        model: object,
        target: str,
        features: Dict[str, List[str]],
        parameters: Dict[str, Any] = None,
    ):
        super().__init__(
            target=target,
            features=features,
            model=model,
            parameters=parameters,
        )

    def metrics(self, y_true: np.array, y_pred: np.array, **kwargs) -> Dict[str, float]:
        return {
            "MAE": mean_absolute_error(y_true, y_pred),
            "MAPE": mean_absolute_percentage_error(y_true, y_pred),
            "RMSE": root_mean_squared_error(y_true, y_pred),
            "R2": r2_score(y_true, y_pred),
        }


class ModelFactory:
    @staticmethod
    def create_model_instance_from_config(
        model_config: ModelConfigSchema,
    ) -> BaseMLModel:
        """Given a model deployment configuration, instantiate the corresponding model release."""
        module = importlib.import_module(model_config.import_path)
        return module.get_model(model_config.model_name)()
