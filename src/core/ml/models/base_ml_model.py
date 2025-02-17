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
from mlflow.models import model, set_model, signature
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
        target_dependencies_as_features: Optional[List[str]] = None,
        parameters: Dict[str, Any] = None,
        signature: Optional[signature.ModelSignature] = None,
    ):
        """Initialization function for the BaseMLModel class.

        Args:
            model: The backend model that actually generates the prediction (ie: sklearn DecisionTreeClassifier).
            target: The target of the model prediction function.
            features: The features used for model prediction. These are a dictionary of the feature source as keys
                and list of feature columns for the source as values.
            target_dependencies_as_features: Other model target values that are used as features in prediction.
                For example, to generate an aspect prediction, we need to know what problem type was predicted.
            parameters: Parameters used in the backend model definition. This is only used for model tracking to
                improve model reproducibility.
            signature: Optional backend model prediction signature. If not provided, the model prediction
                signature is inferred during training.
        """
        self.model = model
        self.target = target
        self.features = features
        self.target_dependencies_as_features = target_dependencies_as_features
        self.parameters = parameters
        self.signature = signature
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

    def model_input_feature_index(self, feature_source: str, feature_name: str) -> int:
        """Given a feature source and name, return the positional index of the column in the feature array."""
        all_sources = list(self.features.keys())
        feature_index = 0
        for source in all_sources:
            if feature_source == source:
                return feature_index + self.features[source].index(feature_name)
            feature_index += len(self.features[source])

    def model_input_target_dependency_as_feature_index(self, target_name: str) -> int:
        """Given a target dependency, return the positional index of the column in the feature array."""
        num_features = 0
        for features_by_source in self.features.values():
            num_features += len(features_by_source)
        return num_features + self.target_dependencies_as_features.index(target_name)

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
        if self.target_dependencies_as_features:
            with db_resource.get_connection() as conn:
                target_dependencies_as_features = conn.execute(
                    f"""
                    SELECT
                        forecast_date,
                        {', '.join(self.target_dependencies_as_features)}
                    FROM targets.target
                    WHERE region_id = $region_id
                        AND forecast_date::DATE < $season_start
                    ORDER BY forecast_date
                    """,
                    {
                        "region_id": region_id,
                        "season_start": season_start,
                    },
                ).df()
                target_dependencies_as_features["forecast_date"] = pd.to_datetime(
                    target_dependencies_as_features["forecast_date"]
                ).dt.date
                feature_dfs.append(target_dependencies_as_features)
        return reduce(
            lambda x, y: pd.merge(x, y, on="forecast_date", how="inner"), feature_dfs
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
        feature_df = reduce(lambda x, y: pd.merge(x, y, hpw="inner"), feature_dfs)
        if self.target_dependencies_as_features:
            feature_df[[f"target.{c}" for c in self.target_dependencies_as_features]] = (
                np.nan
            )
        return feature_df

    def _filter_avalanche_forecast_center_features_by_predicted_problem_type(
        self,
        problem_number: int,
        X: np.array,
    ) -> np.array:
        """Filters the X input array to features matching the predicted problem type, filling with 0s if no match.

        Some models have dependencies on the predicted problem type for the day. For example, an aspect forecast
        depends on the current day's problem type forecast - a wind slab will apply to different aspects than a
        wet slab. Given this relationship, we often only care about avalanche forecast features that relate to
        the current day's problem type prediction. This method searches the input feature set and limits the
        features to only the ones that match the current day's problem type prediction. If the feature set does
        not have any problem types matching the current day's problem type prediction, then feature values of 0
        are used.

        Example:
            problem_number: 0
            X:
                | predicted_problem_type_0 | problem_0 | range_0 | problem_1 | range_1 | problem_2 | range_2 |
                ----------------------------------------------------------------------------------------
                | 1.0                      | 1.0       | 0.5     | 2.0       | 0.8     | 3.0       | 0.1     |
                | 4.0                      | 2.0       | 0.3     | 3.0       | 0.6     | 4.0       | 0.4     |
                | 1.0                      | 1.0       | 0.7     | 2.0       | 0.5     | 3.0       | 0.2     |
                | 7.0                      | 5.0       | 0.2     | 1.0       | 0.4     | 2.0       | 0.6     |
            Would produce the result:
                | predicted_problem_type_0 | range |
                ----------------------------------------------------------------------------------------
                | 1.0                      | 0.5   |  # match on problem_0
                | 4.0                      | 0.4   |  # match on problem_2
                | 1.0                      | 0.7   |  # match on problem_0
                | 7.0                      | 0.0   |  # no match

        Args:
            problem_number: The problem number of the predicted problem type.
            X: The input feature array.

        Returns:
            np.ndarray of the column filtered input feature array.
        """
        predicted_problem_type_col = (
            self.model_input_target_dependency_as_feature_index(
                target_name=f"problem_type_{problem_number}"
            )
        )

        # Iterate over features of each of the 3 problems to see if any match today's problem type prediction.
        conditions = []
        choices = []
        feature_source = "avalanche_forecast_center_feature"
        for i in range(3):
            problem_type_col = self.model_input_feature_index(
                feature_source=feature_source, feature_name=f"problem_type_{i}"
            )
            # Get all the feature names that relate to the current problem under iteration.
            features_of_interest = [
                feature
                for feature in self.features[feature_source]
                if feature.endswith(f"_{i}") and feature != f"problem_type_{i}"
            ]
            # Get the column numbers for the feature names relating to the current problem under iteration.
            X_columns_for_condition = [predicted_problem_type_col] + [
                self.model_input_feature_index(
                    feature_source=feature_source, feature_name=feature
                )
                for feature in features_of_interest
            ]
            # Sub-select X to the features for the current problem type
            choices.append(X[:, X_columns_for_condition])
            # Mark all rows where the problem under iteration matches the predicted problem type.
            condition = X[:, problem_type_col] == X[:, predicted_problem_type_col]
            # Transform the condition array from a 1d array to a 2d array where each row is all True or False
            # depending on whether the above condition was met.
            condition = condition[np.newaxis].T.repeat(
                len(X_columns_for_condition), axis=1
            )
            conditions.append(condition)

        # Create a default array in the case where the predicted problem type was not in the input features.
        # The default array has the predicted type as the first column and all 0s otherwise.
        default_array = np.zeros_like(choices[0])
        default_array[:, 0] = X[:, predicted_problem_type_col]
        # Select the features for each row according to the matching problem type.
        return np.select(conditions, choices, default=default_array)


class BaseMLModelClassification(BaseMLModel):
    def __init__(
        self,
        model: object,
        target: str,
        features: Dict[str, List[str]],
        classes: List[Any],
        target_dependencies_as_features: Optional[List[str]] = None,
        parameters: Dict[str, Any] = None,
        signature: Optional[signature.ModelSignature] = None,
    ):
        super().__init__(
            target=target,
            features=features,
            target_dependencies_as_features=target_dependencies_as_features,
            model=model,
            parameters=parameters,
            signature=signature,
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
