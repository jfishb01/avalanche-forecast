import numpy as np
from typing import Optional
from mlflow.pyfunc import PythonModelContext
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

    problem_number = model_name.split("_")[-1]

    class AspectComponentMLModelClass(BaseMLModelRegression):
        def __init__(self):
            super().__init__(
                target=model_name,
                features={
                    "avalanche_forecast_center_feature": [
                        f"problem_{problem_number}",
                        f"aspect_sin_alp_{problem_number}",
                        f"aspect_cos_alp_{problem_number}",
                        f"aspect_range_alp_{problem_number}",
                        f"aspect_sin_tln_{problem_number}",
                        f"aspect_cos_tln_{problem_number}",
                        f"aspect_range_tln_{problem_number}",
                        f"aspect_sin_btl_{problem_number}",
                        f"aspect_cos_btl_{problem_number}",
                        f"aspect_range_btl_{problem_number}",
                    ]
                },
                model=LinearRegression(),
            )

        def predict(
            self,
            context: Optional[PythonModelContext],
            model_input: np.array,
            params: Optional[dict] = None,
        ) -> np.array:
            """Generates a forecast for the aspect component for the corresponding elevation and problem number.

            If `problem_type` is in the params dictionary and has a value of NOFORECAST, then the predicted
            value will be 0.0. This only applies during prediction as the `problem_type` params argument will
            be left empty during model training.
            """
            params = params or {}
            if params.get("problem_type", -1) == AvalancheProblemEnum.NOFORECAST:
                return [0.0]

            return super().predict(None, model_input)

    return AspectComponentMLModelClass
