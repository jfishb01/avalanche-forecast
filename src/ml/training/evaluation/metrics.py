"""ML training metrics calculations for classifier and regressor models."""

from typing import Dict
from sklearn import metrics
from src.schemas.ml.forecast_model import ForecastModel, ModelTypeEnum


def get_evaluation_metrics(forecast_model: ForecastModel) -> Dict[str, float]:
    """Get all evaluation metrics as a dictionary according to the model type."""
    if forecast_model.predictive_model_type == ModelTypeEnum.CLASSIFIER:
        return _get_evaluation_classifier_metrics(forecast_model)
    else:
        raise NotImplementedError


def _get_evaluation_classifier_metrics(
    forecast_model: ForecastModel,
) -> Dict[str, float]:
    eval_metrics_aggregate = {}
    eval_metrics_by_area = {}
    evaluation_set = forecast_model.evaluation
    for area in forecast_model.forecast_areas:
        evaluation_set_by_area = evaluation_set[
            (evaluation_set["distributor"] == area.distributor)
            & (evaluation_set["area_id"] == area.area_id)
        ]
        eval_metrics_by_area[f"accuracy - {area.distributor}.{area.area_name}"] = round(
            metrics.accuracy_score(
                evaluation_set_by_area["observed"], evaluation_set_by_area["predicted"]
            ),
            3,
        )

    eval_metrics_aggregate["accuracy"] = round(
        metrics.accuracy_score(evaluation_set["observed"], evaluation_set["predicted"]),
        3,
    )
    eval_metrics_aggregate["min_accuracy"] = min(eval_metrics_by_area.values())
    eval_metrics_aggregate["max_accuracy"] = max(eval_metrics_by_area.values())
    return dict(**eval_metrics_aggregate, **eval_metrics_by_area)
