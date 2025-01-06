import numpy as np
import pandas as pd
from datetime import date
from typing import Tuple
from dagster_duckdb import DuckDBResource
from mlflow.models import infer_signature

from src.core.ml.models.base_ml_model import BaseMLModel
from src.utils.datetime_helpers import (
    get_prior_avalanche_season,
    get_avalanche_season_date_bounds,
)


def train_model(
    model: BaseMLModel,
    avalanche_season: str,
    region_id: str,
    db_resource: DuckDBResource,
) -> Tuple[np.array, np.array]:
    """Train the model for a given season and region and return a tuple of the observed values and predictions."""
    features = model.get_features_train(avalanche_season, region_id, db_resource)
    targets = model.get_targets_train(avalanche_season, region_id, db_resource)
    test_start_date, _ = get_avalanche_season_date_bounds(
        get_prior_avalanche_season(avalanche_season)
    )
    X_train, X_test, y_train, y_test = train_test_split(
        features, targets, test_start_date
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(None, X_test)

    # Infer the model signature
    model.signature = infer_signature(X_test, y_pred)

    return y_test, y_pred


def train_test_split(
    features: pd.DataFrame, targets: pd.DataFrame, test_start_date: date
) -> Tuple[
    np.array,
    np.array,
    np.array,
    np.array,
]:
    """Splits the train/test set into X_train, X_test, y_train, y_test components."""
    X_train_test = features.merge(targets[["forecast_date"]])
    X_train = X_train_test[X_train_test["forecast_date"] < test_start_date].drop(
        columns="forecast_date"
    )
    X_test = X_train_test[X_train_test["forecast_date"] >= test_start_date].drop(
        columns="forecast_date"
    )

    y_train_test = targets.merge(features[["forecast_date"]])
    y_train = y_train_test[y_train_test["forecast_date"] < test_start_date].drop(
        columns="forecast_date"
    )
    y_test = y_train_test[y_train_test["forecast_date"] >= test_start_date].drop(
        columns="forecast_date"
    )

    # If there's only one column in the target dataset, convert it to a series so that it's not a
    # multidimensional array
    if len(y_train.columns) == 1:
        y_train = y_train[y_train.columns[0]]
        y_test = y_test[y_test.columns[0]]

    return (
        X_train.values.astype(np.float32),
        X_test.values.astype(np.float32),
        y_train.values.astype(np.float32),
        y_test.values.astype(np.float32),
    )
