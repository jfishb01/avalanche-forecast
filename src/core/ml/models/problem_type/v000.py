"""Use the prior day's problem type forecast published by the avalanche forecast center as features."""

import numpy as np
from typing import Optional
from mlflow.pyfunc import PythonModelContext
from sklearn.tree import DecisionTreeClassifier

from src.schemas.ingestion.avalanche_forecast_center_schemas import AvalancheProblemEnum
from src.core.ml.models.base_ml_model import BaseMLModelClassification


def get_model(model_name: str):
    return {
        "problem_type_0": MlModelProblemType0,
        "problem_type_1": MlModelProblemType1,
        "problem_type_2": MlModelProblemType2,
    }[model_name]


class MlModelProblemType0(BaseMLModelClassification):
    """Decision tree classifier model for problem type 0.

    Uses the prior day's forecast published by the avalanche forecast center as a feature.
    """

    def __init__(self):
        parameters = dict(random_state=42)
        super().__init__(
            target="problem_type_0",
            features={"avalanche_forecast_center_feature": ["problem_type_0"]},
            model=DecisionTreeClassifier(**parameters),
            classes=[problem.value for problem in AvalancheProblemEnum],
            parameters=parameters,
        )


class MlModelProblemType1(BaseMLModelClassification):
    """Decision tree classifier model for problem type 1.

    Uses the prior day's forecast published by the avalanche forecast center as a feature.
    """

    def __init__(self):
        parameters = dict(random_state=np.random.RandomState(seed=42))
        super().__init__(
            target="problem_type_1",
            features={
                "avalanche_forecast_center_feature": [
                    "problem_type_0",
                    "problem_type_1",
                    "problem_type_2",
                ]
            },
            model=DecisionTreeClassifier(**parameters),
            classes=[problem.value for problem in AvalancheProblemEnum],
            parameters=parameters,
        )

    def predict(
        self,
        context: Optional[PythonModelContext],
        model_input: np.array,
        params: Optional[dict] = None,
    ) -> np.array:
        """Generates a forecast for problem_type_1.

        If 'problem_type_0' is in the params dictionary, this method will repeatedly retry until the forecast is
        distinct from this value.
        """
        # Only publish the forecast if it is different from problem_type_0. If problem_type_0 is not provided,
        # then default the problem_type_0 forecast to -1 as this value will never be forecasted.
        params = params or {}
        problem_type_0_forecast = params.get("problem_type_0", -1)

        # Problems are listed in order of significance. If there are not more significant problems forecasted,
        # then this problem should have no forecast risk.
        if problem_type_0_forecast == 0:
            return [0]

        # Retry generating the forecast until it is unique from problem_type_0. The random state generator
        # from the init will allow this to generate new forecasts when rerun in iteration, while still being
        # deterministic when reprocessed.
        while True:
            forecast = super().predict(None, model_input)
            if forecast[0] != problem_type_0_forecast:
                return forecast


class MlModelProblemType2(BaseMLModelClassification):
    """Decision tree classifier model for problem type 2.

    Uses the prior day's forecast published by the avalanche forecast center as a feature.
    """

    def __init__(self):
        parameters = dict(random_state=np.random.RandomState(seed=42))
        super().__init__(
            target="problem_type_2",
            features={
                "avalanche_forecast_center_feature": [
                    "problem_type_0",
                    "problem_type_1",
                    "problem_type_2",
                ]
            },
            model=DecisionTreeClassifier(**parameters),
            classes=[problem.value for problem in AvalancheProblemEnum],
            parameters=parameters,
        )

    def predict(
        self,
        context: Optional[PythonModelContext],
        model_input: np.array,
        params: Optional[dict] = None,
    ) -> np.array:
        """Generates a forecast for problem_type_1.

        If 'problem_type_0' or 'problem_type_1' is in the params dictionary, this method will repeatedly retry
        until the forecast produced is distinct from these values.
        """
        # Only publish the forecast if it is different from problem_type_0 or problem_type_1. If these values
        # are not provided, then default them to -1 as that is an invalid forecast value.
        params = params or {}
        problem_type_0_forecast = params.get("problem_type_0", -1)
        problem_type_1_forecast = params.get("problem_type_1", -1)

        # Problems are listed in order of significance. If there are not more significant problems forecasted,
        # then this problem should have no forecast risk.
        if problem_type_0_forecast == 0 or problem_type_1_forecast == 0:
            return [0]

        # Retry generating the forecast until it is unique from the other forecasts. The random state generator
        # from the init will allow this to generate new forecasts when rerun in iteration, while still being
        # deterministic when reprocessed.
        while True:
            forecast = super().predict(None, model_input)
            if (
                forecast[0] != problem_type_0_forecast
                and forecast[0] != problem_type_1_forecast
            ):
                return forecast
