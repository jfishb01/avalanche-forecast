"""Use the prior day's problem type and aspect forecasts published by the avalanche forecast center forecasts
as features for aspect prediction, filtering the features to only those that match the current day's problem
type prediction.

This improves upon the prior version by sub-selecting the features to those that match the current day's problem
type prediction. In the prior version, all features were used regardless of whether they related to the current
problem type prediction. For example, in the prior version, aspects related to wet slabs were used as features
when predicting aspects for wind slabs. Now, the features are filtered so that only the wind slab aspect features
are used as features in wind slab aspect prediction.
"""

import numpy as np
from typing import Optional
from itertools import chain
from mlflow.pyfunc import PythonModelContext
from mlflow.models import infer_signature
from sklearn.linear_model import LinearRegression

from src.core.ml.models.base_ml_model import BaseMLModelRegression
from src.schemas.ingestion.avalanche_forecast_center_schemas import AvalancheProblemEnum


def get_model(model_name: str):
    """Get the corresponding aspect component model class according to the model name."""
    valid_model_names = [
        f"aspect_{component}_{elevation}_{problem_number}"
        for component in ("sin", "cos", "range")
        for elevation in ("alp", "tln", "btl")
        for problem_number in range(3)
    ]
    if model_name not in valid_model_names:
        raise ValueError(
            f"invalid model name: '{model_name}'. Must be one of {valid_model_names}."
        )

    problem_number = int(model_name.split("_")[-1])

    class AspectComponentMLModelClass(BaseMLModelRegression):
        def __init__(self):
            super().__init__(
                target=model_name,
                features={
                    "avalanche_forecast_center_feature": list(
                        chain.from_iterable(
                            [
                                (
                                    f"problem_type_{i}",
                                    f"aspect_sin_alp_{i}",
                                    f"aspect_cos_alp_{i}",
                                    f"aspect_range_alp_{i}",
                                    f"aspect_sin_tln_{i}",
                                    f"aspect_cos_tln_{i}",
                                    f"aspect_range_tln_{i}",
                                    f"aspect_sin_btl_{i}",
                                    f"aspect_cos_btl_{i}",
                                    f"aspect_range_btl_{i}",
                                )
                                for i in range(3)
                            ]
                        )
                    ),
                },
                target_dependencies_as_features=[
                    f"problem_type_{problem_number}",
                ],
                model=LinearRegression(),
                signature=infer_signature(np.zeros((10,)), np.zeros((1,))),
            )

        def fit(self, X_train: np.array, y_train: np.array) -> None:
            """Fit the model to the training set with feature sub-selection using the predicted problem type."""
            X_train = self._filter_avalanche_forecast_center_features_by_predicted_problem_type(
                problem_number=problem_number, X=X_train
            )
            self.model.fit(X_train, y_train)

        def predict(
            self,
            context: Optional[PythonModelContext],
            model_input: np.array,
            params: Optional[dict] = None,
        ) -> np.array:
            """Generates a forecast for the aspect component for the corresponding elevation and problem number.

            The 'problem_type_X' param will only be provided during live prediction and comes from the upstream
            model forecasting the problem type for the day. If `problem_type_X`  has a value of NOFORECAST,
            then the predicted value will be 0.0. Otherwise, the 'problem_type_X' parameter will be added to
            the model_input array. Features are then sub-selected to whether they match the predicted problem type
            for the day.
            """
            problem_type_dependency_col = (
                self.model_input_target_dependency_as_feature_index(
                    f"problem_type_{problem_number}"
                )
            )
            params = params or {}
            if f"problem_type_{problem_number}" in params:
                if (
                    params[f"problem_type_{problem_number}"]
                    == AvalancheProblemEnum.NOFORECAST
                ):
                    return [0.0]

                model_input[:, problem_type_dependency_col] = params[
                    f"problem_type_{problem_number}"
                ]

            model_input = self._filter_avalanche_forecast_center_features_by_predicted_problem_type(
                problem_number=problem_number, X=model_input
            )
            return super().predict(None, model_input)

    return AspectComponentMLModelClass
