from sklearn.tree import DecisionTreeClassifier

from src.core.ml.models.base_ml_model import BaseMLModelClassification


class MlModelProblem0(BaseMLModelClassification):
    """Decision tree classifier model for problem 0.

    Uses the prior day's forecast published by the avalanche forecast center as a feature.
    """

    def __init__(self):
        parameters = dict(random_state=42)
        super().__init__(
            target="problem_0",
            features={"avalanche_forecast_center_feature": ["problem_0"]},
            model=DecisionTreeClassifier(**parameters),
            parameters=parameters,
        )
