"""Helper methods to support the ML training process."""


import os
import re
import pickle
import tempfile
import mlflow
import argparse
import subprocess
import pandas as pd
import pandera as pa
from pandera.typing import DataFrame
from datetime import date
from pydantic import BaseModel
from typing import Dict, Union, Tuple, Sequence

from src.ml.training.evaluation.metrics import get_evaluation_metrics
from src.ml.training.evaluation.visuals import save_evaluation_plots
from src.ml.model_helpers import get_features, get_targets
from src.schemas.ml.forecast_model import (
    ForecastModel,
    ModelFeatureWithTargetSchema,
    ModelFeatureSchema,
    ModelTargetSchema,
)
from src.ingestion.avalanche_forecast.ingestion_helpers import ForecastDistributorEnum


class ApiQueryParams(BaseModel):
    distributors: Sequence[ForecastDistributorEnum]
    train_start_analysis_date: date = date(1900, 1, 1)
    train_end_analysis_date: date
    test_end_analysis_date: date
    feature_db_uri: str
    mlflow_server_uri: str


def get_train_test_set(
    db_uri: str,
    distributors: Sequence[ForecastDistributorEnum],
    train_start_analysis_date: date,
    test_end_analysis_date: date,
    features: Sequence[str],
    target: str,
    fill_values: Union[int, float, Dict[str, Union[int, float]]] = 0,
) -> pd.DataFrame:
    """Retrieve the full dataset over the period to use for training and testing.

    Args:
        db_uri: The URI of the database backend.
        distributors: Distributors to use in the training/testing process.
        train_start_analysis_date: The start analysis date to use (beginning of the training set).
        test_end_analysis_date: The end analysis date to use (end of the test set).
        features: Set of features to consider during the training process
        fill_values: Values to use to fill any missing values in the dataset. If a scalar is provided,
            all values will be filled with the scalar. If a dict, is provided, matching columns will be filled
            with the corresponding dictionary values. Any unmatched columns will be filled with 0.

    Returns: The combined train/test dataset.
    """
    feature_df = get_features(
        db_uri,
        distributors,
        train_start_analysis_date,
        test_end_analysis_date,
        features,
        fill_values,
    )
    feature_cols = feature_df.columns
    target_df = get_targets(
        db_uri, distributors, train_start_analysis_date, test_end_analysis_date, target
    )
    target_cols = target_df.columns
    return (
        feature_df.merge(
            target_df,
            left_on=["distributor", "area_id", "forecast_date"],
            right_on=["distributor", "area_id", "observation_date"],
            suffixes=("", "_target"),
        )[list(set(feature_cols).union(target_cols))]
        .sort_values(["distributor", "area_id", "analysis_date"])
        .reset_index(drop=True)
    )


@pa.check_types
def get_train_test_split(
    features_with_target: ModelFeatureWithTargetSchema, train_end_analysis_date: date
) -> Tuple[
    DataFrame[ModelFeatureSchema],
    DataFrame[ModelFeatureSchema],
    DataFrame[ModelTargetSchema],
    DataFrame[ModelTargetSchema],
]:
    """Splits the train/test set into X_train, X_test, y_train, y_test components."""
    y_cols = ["distributor", "area_id", "area_name", "observation_date", "target"]
    X_train = features_with_target[
        features_with_target["analysis_date"] <= train_end_analysis_date
    ].reset_index(drop=True)
    X_test = features_with_target[
        (features_with_target["analysis_date"] > train_end_analysis_date)
    ].reset_index(drop=True)
    return (
        X_train.drop(columns=["target", "observation_date"]),
        X_test.drop(columns=["target", "observation_date"]),
        X_train[y_cols],
        X_test[y_cols],
    )


def log_model_to_mlflow(forecast_model: ForecastModel) -> None:  # pragma: no cover
    """Log the model and its corresponding evaluation metrics to MlFlow."""
    mlflow.set_tracking_uri(uri="http://172.17.0.1:5000")
    mlflow.set_experiment(forecast_model.experiment_name)
    with mlflow.start_run():
        mlflow.set_tag("distributors", str(sorted(forecast_model.distributors)))
        mlflow.log_params(
            dict(
                git_commit_hash=get_current_git_commit_hash(),
                predictive_model_type=forecast_model.predictive_model_type.name,
                distributors=forecast_model.distributors,
                target=forecast_model.target,
                features=forecast_model.features,
                train_start_analysis_date=forecast_model.X_train["analysis_date"].min(),
                train_end_analysis_date=forecast_model.X_train["analysis_date"].max(),
                hyperparameters=forecast_model.hyperparameters,
            )
        )
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(pickle.dumps(forecast_model.trained_model))
            mlflow.log_artifact(tmp.name, "trained_model.pkl")
        eval_metrics = get_evaluation_metrics(forecast_model)
        chars_to_sanitize = r"[^A-Za-z0-9_. -]"
        mlflow.log_metrics(
            {re.sub(chars_to_sanitize, "", k): v for k, v in eval_metrics.items()}
        )
        with tempfile.TemporaryDirectory() as tmp:
            forecast_model.evaluation.to_csv(os.path.join(tmp, "evaluation.csv"))
            save_evaluation_plots(tmp, forecast_model)
            mlflow.log_artifacts(tmp)


def get_current_git_commit_hash() -> str:  # pragma: no cover
    """Get the active git commit hash."""
    return subprocess.check_output(["git", "rev-parse", "HEAD"]).decode("ascii").strip()


def parse_cli() -> ApiQueryParams:  # pragma: no cover
    """Read a standardized set CLI parameters for the model training process."""
    distributors_str = "\n\t".join(list(ForecastDistributorEnum))
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--distributors",
        dest="distributors",
        action="store",
        required=True,
        help=f"Comma separated list of forecast distributors for training models.\n\tOptions: "
        f"{distributors_str}",
    )
    parser.add_argument(
        "--train-start",
        dest="train_start_analysis_date",
        action="store",
        required=False,
        default="2000-01-01",
        help="Training set start analysis date",
    )
    parser.add_argument(
        "--train-end",
        dest="train_end_analysis_date",
        action="store",
        required=True,
        help="Training set end analysis date. This is also the train/test cutoff date",
    )
    parser.add_argument(
        "--test-end",
        dest="test_end_analysis_date",
        action="store",
        required=True,
        help="Test set end analysis date.",
    )
    parser.add_argument(
        "--feature-db-uri",
        dest="feature_db_uri",
        action="store",
        required=False,
        default="clickhouse+native://172.17.0.1:19000",
        help="Database URI for grabbing features",
    )
    parser.add_argument(
        "--mlflow-server-uri",
        dest="mlflow_server_uri",
        action="store",
        required=False,
        default="http://172.17.0.1:5000",
        help="MlFlow server URI for storing the model and associated metrics",
    )
    args = parser.parse_args()
    return ApiQueryParams(
        distributors=args.distributors.split(","),
        train_start_analysis_date=args.train_start_analysis_date,
        train_end_analysis_date=args.train_end_analysis_date,
        test_end_analysis_date=args.test_end_analysis_date,
        feature_db_uri=args.feature_db_uri,
        mlflow_server_uri=args.mlflow_server_uri,
    )
