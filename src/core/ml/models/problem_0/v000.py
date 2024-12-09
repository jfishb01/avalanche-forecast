import numpy as np
from typing import Dict
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

from src.core.ml.models.base_ml_model import BaseMLModel


class MlModelProblem0(BaseMLModel):
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

    def metrics(self, y_true: np.array, y_pred: np.array, **kwargs) -> Dict[str, float]:
        return {
            "accuracy_score": accuracy_score(y_true, y_pred),
            "precision_score": precision_score(y_true, y_pred, average="macro"),
            "recall_score": recall_score(y_true, y_pred, average="macro"),
            "f1_score": f1_score(y_true, y_pred, average="macro"),
        }
