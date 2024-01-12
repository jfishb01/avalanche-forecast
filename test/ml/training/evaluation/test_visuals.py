import os
import pytest
import numpy as np
import pandas as pd
from unittest.mock import patch, call
from datetime import date, timedelta

from src.schemas.ml.forecast_model import (
    ForecastModel,
    ModelTypeEnum,
    ForecastArea,
    ModelFeatureSchema,
    ModelTargetSchema,
)
from src.ml.training.evaluation.visuals import (
    save_evaluation_plots,
    save_evaluation_accuracy_plot,
    ScatterPlotCategory,
    OBSERVED_MARKER,
    PREDICTED_MARKER,
    CORRECT_MARKER,
)


class TestMlModel:
    def predict(self, features: pd.DataFrame) -> np.array:
        return features["test"].values


def test_save_evaluation_plots__classifier():
    model = ForecastModel(
        experiment_name="test",
        mlflow_uri="http://test",
        predictive_model_type=ModelTypeEnum.CLASSIFIER,
        distributors=[],
        target="test",
        trained_model=TestMlModel(),
        features=["test"],
        X_train=pd.DataFrame(columns=ModelFeatureSchema.to_schema().columns),
        X_test=pd.DataFrame(columns=ModelFeatureSchema.to_schema().columns),
        y_train=pd.DataFrame(columns=ModelTargetSchema.to_schema().columns),
        y_test=pd.DataFrame(columns=ModelTargetSchema.to_schema().columns),
    )
    with patch(
        "src.ml.training.evaluation.visuals.save_evaluation_classifier_plots"
    ) as m:
        output_dir = "/tmp/test_dir"
        save_evaluation_plots(output_dir, model)
        m.assert_called_once_with(output_dir, model)


def test_save_evaluation_plots__regressor():
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
        save_evaluation_plots("/tmp/test_dir", model)


@pytest.mark.parametrize(
    "desc,y_test",
    [
        (
            "Accuracy plot is created for each distributor forecast area",
            pd.DataFrame(
                {
                    "observation_date": [
                        d + timedelta(days=i)
                        for i, d in enumerate([date(2000, 1, 1)] * 4)
                    ],
                    "distributor": ["CAIC", "CAIC", "NWAC", "NWAC"],
                    "area_name": ["area_0", "area_1", "area_0", "area_1"],
                    "area_id": ["100", "101", "100", "101"],
                    "target": [1] * 4,
                }
            ),
        ),
        (
            "No accuracy plots created if no forecast areas",
            pd.DataFrame(columns=ModelTargetSchema.to_schema().columns),
        ),
    ],
)
def test_save_evaluation_classifier_plots(desc, y_test):
    model = ForecastModel(
        experiment_name="test",
        mlflow_uri="http://test",
        predictive_model_type=ModelTypeEnum.CLASSIFIER,
        distributors=[],
        target="test",
        trained_model=TestMlModel(),
        features=["test"],
        X_train=pd.DataFrame(columns=ModelFeatureSchema.to_schema().columns),
        X_test=pd.DataFrame(columns=ModelFeatureSchema.to_schema().columns),
        y_train=pd.DataFrame(columns=ModelTargetSchema.to_schema().columns),
        y_test=y_test,
    )
    output_dir = "/tmp/test_dir"
    calls = [
        call(
            os.path.join(output_dir, f"{area.distributor}: {area.area_name}.svg"),
            model,
            area,
        )
        for area in model.forecast_areas
    ]
    with patch("src.ml.training.evaluation.visuals.save_evaluation_accuracy_plot") as m:
        save_evaluation_plots(output_dir, model)
        m.assert_has_calls(calls, any_order=True)


def test_save_evaluation_accuracy_plot__forecast_area_missing_from_evaluation_set():
    model = ForecastModel(
        experiment_name="test",
        mlflow_uri="http://test",
        predictive_model_type=ModelTypeEnum.CLASSIFIER,
        distributors=[],
        target="test",
        trained_model=TestMlModel(),
        features=["test"],
        X_train=pd.DataFrame(columns=ModelFeatureSchema.to_schema().columns),
        X_test=pd.DataFrame(
            {
                "analysis_date": [date(2000, 1, 1)],
                "forecast_date": [date(2000, 1, 1)],
                "distributor": ["CAIC"],
                "area_name": ["area_0"],
                "area_id": ["100"],
                "test": [1],
            }
        ),
        y_train=pd.DataFrame(columns=ModelTargetSchema.to_schema().columns),
        y_test=pd.DataFrame(
            {
                "observation_date": [date(2000, 1, 1)],
                "distributor": ["CAIC"],
                "area_name": ["area_0"],
                "area_id": ["100"],
                "target": [1],
            }
        ),
    )
    with pytest.raises(AssertionError):
        save_evaluation_accuracy_plot(
            "tmp/test_output.svg",
            model,
            ForecastArea(distributor="NWAC", area_name="test", area_id="1"),
        )


def test_save_evaluation_accuracy_plot__show_correct_forecasts_is_true():
    model = ForecastModel(
        experiment_name="test",
        mlflow_uri="http://test",
        predictive_model_type=ModelTypeEnum.CLASSIFIER,
        distributors=[],
        target="test",
        trained_model=TestMlModel(),
        features=["test"],
        X_train=pd.DataFrame(columns=ModelFeatureSchema.to_schema().columns),
        X_test=pd.DataFrame(
            {
                "analysis_date": [
                    d + timedelta(days=i) for i, d in enumerate([date(2000, 1, 1)] * 3)
                ],
                "forecast_date": [
                    d + timedelta(days=i) for i, d in enumerate([date(2000, 1, 1)] * 3)
                ],
                "distributor": ["CAIC"] * 3,
                "area_name": ["area_0", "area_0", "area_1"],
                "area_id": ["100", "100", "101"],
                "test": [1, 2, 1],
            }
        ),
        y_train=pd.DataFrame(columns=ModelTargetSchema.to_schema().columns),
        y_test=pd.DataFrame(
            {
                "observation_date": [
                    d + timedelta(days=i) for i, d in enumerate([date(2000, 1, 1)] * 3)
                ],
                "distributor": ["CAIC"] * 3,
                "area_name": ["area_0", "area_0", "area_1"],
                "area_id": ["100", "100", "101"],
                "target": [1] * 3,
            }
        ),
    )
    with patch("src.ml.training.evaluation.visuals._save_scatter_plot") as m:
        output_path = "/tmp/test_output.svg"
        save_evaluation_accuracy_plot(
            output_path,
            model,
            ForecastArea(distributor="CAIC", area_name="area_0", area_id="100"),
        )
        m.assert_called_once_with(
            output_path,
            "test: CAIC - area_0",
            "test",
            (
                ScatterPlotCategory([date(2000, 1, 2)], [1.0], OBSERVED_MARKER),
                ScatterPlotCategory([date(2000, 1, 2)], [2.0], PREDICTED_MARKER),
                ScatterPlotCategory([date(2000, 1, 1)], [1.0], CORRECT_MARKER),
            ),
        )


def test_save_evaluation_accuracy_plot__show_correct_forecasts_is_false():
    model = ForecastModel(
        experiment_name="test",
        mlflow_uri="http://test",
        predictive_model_type=ModelTypeEnum.CLASSIFIER,
        distributors=[],
        target="test",
        trained_model=TestMlModel(),
        features=["test"],
        X_train=pd.DataFrame(columns=ModelFeatureSchema.to_schema().columns),
        X_test=pd.DataFrame(
            {
                "analysis_date": [
                    d + timedelta(days=i) for i, d in enumerate([date(2000, 1, 1)] * 3)
                ],
                "forecast_date": [
                    d + timedelta(days=i) for i, d in enumerate([date(2000, 1, 1)] * 3)
                ],
                "distributor": ["CAIC"] * 3,
                "area_name": ["area_0", "area_0", "area_1"],
                "area_id": ["100", "100", "101"],
                "test": [1, 2, 1],
            }
        ),
        y_train=pd.DataFrame(columns=ModelTargetSchema.to_schema().columns),
        y_test=pd.DataFrame(
            {
                "observation_date": [
                    d + timedelta(days=i) for i, d in enumerate([date(2000, 1, 1)] * 3)
                ],
                "distributor": ["CAIC"] * 3,
                "area_name": ["area_0", "area_0", "area_1"],
                "area_id": ["100", "100", "101"],
                "target": [1] * 3,
            }
        ),
    )
    with patch("src.ml.training.evaluation.visuals._save_scatter_plot") as m:
        output_path = "/tmp/test_output.svg"
        save_evaluation_accuracy_plot(
            output_path,
            model,
            ForecastArea(distributor="CAIC", area_name="area_0", area_id="100"),
            show_correct_forecasts=False,
        )
        m.assert_called_once_with(
            output_path,
            "test: CAIC - area_0",
            "test",
            (
                ScatterPlotCategory([date(2000, 1, 2)], [1.0], OBSERVED_MARKER),
                ScatterPlotCategory([date(2000, 1, 2)], [2.0], PREDICTED_MARKER),
                None,
            ),
        )
