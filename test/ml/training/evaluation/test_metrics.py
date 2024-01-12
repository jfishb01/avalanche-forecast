import pytest
import numpy as np
import pandas as pd
from datetime import date, timedelta

from src.schemas.ml.forecast_model import (
    ForecastModel,
    ModelTypeEnum,
    ModelFeatureSchema,
    ModelTargetSchema,
)
from src.ml.training.evaluation.metrics import get_evaluation_metrics


class TestMlModel:
    def predict(self, features: pd.DataFrame) -> np.array:
        return features["test"].values


@pytest.mark.parametrize(
    "desc,X_test,y_test,expected",
    [
        (
            "Single forecast area produces same set of metrics for forecast area and aggregate",
            pd.DataFrame(
                {
                    "distributor": ["CAIC"] * 2,
                    "area_id": ["100"] * 2,
                    "area_name": ["area_0"] * 2,
                    "analysis_date": [
                        d + timedelta(days=i)
                        for i, d in enumerate([date(2000, 1, 1)] * 2)
                    ],
                    "forecast_date": [
                        d + timedelta(days=i)
                        for i, d in enumerate([date(2000, 1, 1)] * 2)
                    ],
                    "test": [1, 2],
                }
            ),
            pd.DataFrame(
                {
                    "observation_date": [
                        d + timedelta(days=i)
                        for i, d in enumerate([date(2000, 1, 1)] * 2)
                    ],
                    "distributor": ["CAIC"] * 2,
                    "area_name": ["area_0"] * 2,
                    "area_id": ["100"] * 2,
                    "target": [1] * 2,
                }
            ),
            {
                "accuracy": 0.5,
                "min_accuracy": 0.5,
                "max_accuracy": 0.5,
                "accuracy - CAIC.area_0": 0.5,
            },
        ),
        (
            "Multiple forecast areas produce metrics for each forecast area and aggregate",
            pd.DataFrame(
                {
                    "distributor": ["CAIC"] * 5,
                    "area_name": ["area_0", "area_1", "area_1", "area_1", "area_1"],
                    "area_id": ["100", "111", "111", "111", "111"],
                    "analysis_date": [
                        d + timedelta(days=i)
                        for i, d in enumerate([date(2000, 1, 1)] * 5)
                    ],
                    "forecast_date": [
                        d + timedelta(days=i)
                        for i, d in enumerate([date(2000, 1, 1)] * 5)
                    ],
                    "test": [1, 1, 1, 1, 2],
                }
            ),
            pd.DataFrame(
                {
                    "observation_date": [
                        d + timedelta(days=i)
                        for i, d in enumerate([date(2000, 1, 1)] * 5)
                    ],
                    "distributor": ["CAIC"] * 5,
                    "area_name": ["area_0", "area_1", "area_1", "area_1", "area_1"],
                    "area_id": ["100", "111", "111", "111", "111"],
                    "target": [1] * 5,
                }
            ),
            {
                "accuracy": 0.8,
                "min_accuracy": 0.75,
                "max_accuracy": 1.0,
                "accuracy - CAIC.area_0": 1.0,
                "accuracy - CAIC.area_1": 0.75,
            },
        ),
        (
            "Multiple distributors produce metrics for each distributor forecast area and aggregate",
            pd.DataFrame(
                {
                    "distributor": ["CAIC", "NWAC"],
                    "area_id": ["100", "100"],
                    "area_name": ["area_0", "area_1"],
                    "analysis_date": [
                        d + timedelta(days=i)
                        for i, d in enumerate([date(2000, 1, 1)] * 2)
                    ],
                    "forecast_date": [
                        d + timedelta(days=i)
                        for i, d in enumerate([date(2000, 1, 1)] * 2)
                    ],
                    "test": [1, 2],
                }
            ),
            pd.DataFrame(
                {
                    "observation_date": [
                        d + timedelta(days=i)
                        for i, d in enumerate([date(2000, 1, 1)] * 2)
                    ],
                    "distributor": ["CAIC", "NWAC"],
                    "area_name": ["area_0", "area_0"],
                    "area_id": ["100", "100"],
                    "target": [1, 1],
                }
            ),
            {
                "accuracy": 0.5,
                "min_accuracy": 0.0,
                "max_accuracy": 1.0,
                "accuracy - CAIC.area_0": 1.0,
                "accuracy - NWAC.area_0": 0.0,
            },
        ),
    ],
)
def test_get_evaluation_metrics__classifier(desc, X_test, y_test, expected):
    model = ForecastModel(
        experiment_name="test",
        mlflow_uri="http://test",
        predictive_model_type=ModelTypeEnum.CLASSIFIER,
        distributors=list(y_test["distributor"].unique()),
        target="test",
        trained_model=TestMlModel(),
        features=["test"],
        X_train=pd.DataFrame(columns=ModelFeatureSchema.to_schema().columns),
        X_test=X_test,
        y_train=pd.DataFrame(columns=ModelTargetSchema.to_schema().columns),
        y_test=y_test,
    )
    actual = get_evaluation_metrics(model)
    assert actual == expected


def test_get_evaluation_metrics__regressor():
    model = ForecastModel(
        experiment_name="test",
        mlflow_uri="http://test",
        predictive_model_type=ModelTypeEnum.REGRESSOR,
        distributors=[],
        target="test",
        trained_model=TestMlModel(),
        features=["test"],
        X_train=pd.DataFrame(columns=ModelFeatureSchema.to_schema().columns),
        X_test=pd.DataFrame(columns=ModelFeatureSchema.to_schema().columns),
        y_train=pd.DataFrame(columns=ModelTargetSchema.to_schema().columns),
        y_test=pd.DataFrame(columns=ModelTargetSchema.to_schema().columns),
    )
    with pytest.raises(NotImplementedError):
        get_evaluation_metrics(model)


@pytest.mark.parametrize(
    "desc,X_test,y_test",
    [
        (
            "Empty test set raises ValueError",
            pd.DataFrame(columns=ModelFeatureSchema.to_schema().columns),
            pd.DataFrame(columns=ModelTargetSchema.to_schema().columns),
        ),
        (
            "Different sized X_test and y_test raises ValueError",
            pd.DataFrame(
                {
                    "distributor": ["CAIC"],
                    "area_id": ["100"],
                    "area_name": ["area_0"],
                    "analysis_date": [date(2000, 1, 1)],
                    "forecast_date": [date(2000, 1, 1)],
                    "test": [1],
                }
            ),
            pd.DataFrame(
                {
                    "observation_date": [
                        d + timedelta(days=i)
                        for i, d in enumerate([date(2000, 1, 1)] * 2)
                    ],
                    "distributor": ["CAIC", "NWAC"],
                    "area_name": ["area_0", "area_0"],
                    "area_id": ["100", "100"],
                    "target": [1, 1],
                }
            ),
        ),
    ],
)
def test_get_evaluation_metrics__error_cases(desc, X_test, y_test):
    model = ForecastModel(
        experiment_name="test",
        mlflow_uri="http://test",
        predictive_model_type=ModelTypeEnum.CLASSIFIER,
        distributors=list(y_test["distributor"].unique()),
        target="test",
        trained_model=TestMlModel(),
        features=["test"],
        X_train=pd.DataFrame(columns=ModelFeatureSchema.to_schema().columns),
        X_test=X_test,
        y_train=pd.DataFrame(columns=ModelTargetSchema.to_schema().columns),
        y_test=y_test,
    )
    with pytest.raises(ValueError):
        get_evaluation_metrics(model)
