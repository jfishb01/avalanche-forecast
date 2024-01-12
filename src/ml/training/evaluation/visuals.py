"""Create visualizations of model training evaluations for provided forecast models."""

import os
import matplotlib.pyplot as plt
from dataclasses import dataclass, asdict
from typing import Sequence
from src.schemas.ml.forecast_model import (
    ForecastModel,
    ForecastArea,
    ModelTypeEnum,
)


@dataclass
class ScatterPlotMarker:
    label: str
    color: str
    s: int
    marker: str


@dataclass
class ScatterPlotCategory:
    x: Sequence
    y: Sequence
    marker: ScatterPlotMarker


MARKER_STYLE = "|"
MARKER_SIZE = 100
OBSERVED_MARKER = ScatterPlotMarker(
    label="observed", color="blue", s=MARKER_SIZE, marker=MARKER_STYLE
)
CORRECT_MARKER = ScatterPlotMarker(
    label="correct", color="green", s=MARKER_SIZE, marker=MARKER_STYLE
)
PREDICTED_MARKER = ScatterPlotMarker(
    label="predicted", color="red", s=MARKER_SIZE, marker=MARKER_STYLE
)


def save_evaluation_plots(output_dir: str, forecast_model: ForecastModel) -> None:
    """Save training evaluation plots according to the model type."""
    if forecast_model.predictive_model_type == ModelTypeEnum.CLASSIFIER:
        return save_evaluation_classifier_plots(output_dir, forecast_model)
    else:
        raise NotImplementedError


def save_evaluation_classifier_plots(
    output_dir: str, forecast_model: ForecastModel
) -> None:
    """Save classifier training evaluation plots for each forecast area."""
    for area in forecast_model.forecast_areas:
        output_file = f"{area.distributor}: {area.area_name}.svg".replace("/", "")
        save_evaluation_accuracy_plot(
            os.path.join(output_dir, output_file),
            forecast_model,
            area,
        )


def save_evaluation_accuracy_plot(
    output_path: str,
    forecast_model: ForecastModel,
    area: ForecastArea,
    show_correct_forecasts: bool = True,
) -> None:
    """Save a training evaluation accuracy plot for the provided forecast area."""
    area_evaluation_set = forecast_model.evaluation[
        (forecast_model.evaluation["distributor"] == area.distributor)
        & (forecast_model.evaluation["area_id"] == area.area_id)
    ]
    assert len(area_evaluation_set), "Missing forecast area in evaluation set"
    # forecast_dates = area_evaluation_set["forecast_date"]
    error_points = area_evaluation_set[
        area_evaluation_set["observed"] != area_evaluation_set["predicted"]
    ]
    correct_points = area_evaluation_set[
        area_evaluation_set["observed"] == area_evaluation_set["predicted"]
    ]
    return _save_scatter_plot(
        output_path,
        f"{forecast_model.target}: {area.distributor} - {area.area_name}",
        forecast_model.target,
        (
            ScatterPlotCategory(
                error_points["analysis_date"].tolist(),
                error_points["observed"].tolist(),
                OBSERVED_MARKER,
            ),
            ScatterPlotCategory(
                error_points["analysis_date"].tolist(),
                error_points["predicted"].tolist(),
                PREDICTED_MARKER,
            ),
            ScatterPlotCategory(
                correct_points["analysis_date"].tolist(),
                correct_points["observed"].tolist(),
                CORRECT_MARKER,
            )
            if show_correct_forecasts
            else None,
        ),
    )


def _save_scatter_plot(
    output_path: str,
    plot_title: str,
    target: str,
    categories: Sequence[ScatterPlotCategory],
) -> None:  # pragma: no cover
    plt.clf()
    for category in filter(lambda x: bool(x), categories):
        plt.scatter(category.x, category.y, **asdict(category.marker))
    plt.legend()
    plt.ylabel(target)
    plt.xlabel("Analysis Date")
    plt.title(plot_title)
    return plt.savefig(output_path)
