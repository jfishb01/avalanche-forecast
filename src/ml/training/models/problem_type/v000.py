"""Heuristic baseline model for avalanche problem type that returns the prior day's forecast as its prediction."""

import numpy as np
import pandas as pd
from fastapi import FastAPI
from mlflow.pyfunc import PythonModel

from src.utils.loggers import set_console_logger
from src.schemas.feature_sets.avalanche_forecast import NUM_POSSIBLE_PROBLEMS
from src.ml.training.training_helpers import (
    parse_cli,
    get_train_test_split,
    get_train_test_set,
    log_model_to_mlflow,
)
from src.schemas.ml.forecast_model import ForecastModel, ModelTypeEnum


app = FastAPI()


class BaselineModel(PythonModel):
    def __init__(self, target):
        self._target = target

    def predict(self, features: pd.DataFrame) -> np.array:
        return features[self._target].values


@app.post("/train")
def train(ApiQueryParams) -> None:
    """Train and record metrics for a baseline model forecasting avalanche problem type.

    Given that this a heuristic model, no actual training occurs as part of this process. This method is named
    "train" for consistency with other trained ML models so that this can easily be compared as a baseline.
    """
    train_start_analysis_date = ApiQueryParams.train_start_analysis_date
    train_end_analysis_date = ApiQueryParams.train_end_analysis_date
    test_end_analysis_date = ApiQueryParams.test_end_analysis_date

    for i in range(NUM_POSSIBLE_PROBLEMS):
        target = f"problem_{i}"
        features = [target]
        train_test_df = get_train_test_set(
            ApiQueryParams.feature_db_uri,
            ApiQueryParams.distributors,
            train_start_analysis_date,
            test_end_analysis_date,
            features,
            target,
        )
        X_train, X_test, y_train, y_test = get_train_test_split(
            train_test_df, train_end_analysis_date
        )
        baseline_model = BaselineModel(target)
        model = ForecastModel(
            experiment_name=f"{target}.v000",
            mlflow_uri=ApiQueryParams.mlflow_server_uri,
            predictive_model_type=ModelTypeEnum.CLASSIFIER,
            distributors=ApiQueryParams.distributors,
            target=target,
            trained_model=baseline_model,
            features=features,
            X_train=X_train,
            X_test=X_test,
            y_train=y_train,
            y_test=y_test,
        )
        log_model_to_mlflow(model)


def main():
    set_console_logger()
    train(parse_cli())


if __name__ == "__main__":
    main()
