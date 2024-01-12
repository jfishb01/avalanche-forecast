"""Dataclasses and schema forecast models."""

import numpy as np
import pandas as pd
import pandera as pa
from datetime import date
from enum import IntEnum, auto
from functools import cached_property
from typing import Protocol, Any, Dict, Sequence, runtime_checkable
from pydantic import BaseModel, ConfigDict
from pandera.typing import Series, DataFrame

from src.schemas.schema_config import (
    FlexibleDataFrameSchemaConfig,
    StrictDataFrameSchemaConfig,
)
from src.ingestion.avalanche_forecast.ingestion_helpers import ForecastDistributorEnum


@runtime_checkable
class BaseForecastModel(Protocol):
    def predict(self, features: pd.DataFrame) -> np.array:  # pragma: no cover
        pass


class ModelTypeEnum(IntEnum):
    CLASSIFIER = auto()
    REGRESSOR = auto()


class ForecastArea(BaseModel):
    distributor: ForecastDistributorEnum
    area_name: str
    area_id: str


class ForecastAreaSchema(pa.DataFrameModel):
    distributor: Series[str]
    area_name: Series[str]
    area_id: Series[str]

    class Config(metaclass=FlexibleDataFrameSchemaConfig):
        pass


class ModelFeatureSchema(ForecastAreaSchema):
    analysis_date: Series[date]
    forecast_date: Series[date]

    class Config(metaclass=FlexibleDataFrameSchemaConfig):
        pass


class ModelTargetSchema(ForecastAreaSchema):
    observation_date: Series[date]
    target: Series[float]

    class Config(metaclass=StrictDataFrameSchemaConfig):
        pass


class ModelFeatureWithTargetSchema(ForecastAreaSchema):
    analysis_date: Series[date]
    forecast_date: Series[date]
    observation_date: Series[date]
    target: Series[float]

    class Config(metaclass=FlexibleDataFrameSchemaConfig):
        pass


class ForecastModelEvaluationSchema(ForecastAreaSchema):
    analysis_date: Series[date]
    forecast_date: Series[date]
    observed: Series[float]
    predicted: Series[float]

    class Config(metaclass=StrictDataFrameSchemaConfig):
        pass


class ForecastModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    experiment_name: str
    mlflow_uri: str
    predictive_model_type: ModelTypeEnum
    distributors: Sequence[ForecastDistributorEnum]
    target: str
    trained_model: BaseForecastModel
    X_train: DataFrame[ModelFeatureSchema]
    X_test: DataFrame[ModelFeatureSchema]
    y_train: DataFrame[ModelTargetSchema]
    y_test: DataFrame[ModelTargetSchema]
    features: Sequence[str]
    hyperparameters: Dict[str, Any] = {}

    @cached_property
    @pa.check_types
    def forecast_areas(self) -> Sequence[ForecastArea]:
        return [
            ForecastArea(**record)
            for record in self.y_test.groupby(["distributor", "area_id"])
            .agg({"area_name": "max"})
            .sort_values(["distributor", "area_name"])
            .reset_index()
            .to_dict(orient="records")
        ]

    @cached_property
    @pa.check_types
    def evaluation(self) -> DataFrame[ForecastModelEvaluationSchema]:
        return pd.concat(
            [
                pd.DataFrame(
                    {
                        "distributor": area.distributor.value,
                        "area_name": area.area_name,
                        "area_id": area.area_id,
                        "analysis_date": self.X_test[
                            (self.X_test["distributor"] == area.distributor)
                            & (self.X_test["area_id"] == area.area_id)
                        ]["analysis_date"].reset_index(drop=True),
                        "forecast_date": self.y_test[
                            (self.y_test["distributor"] == area.distributor)
                            & (self.y_test["area_id"] == area.area_id)
                        ]["observation_date"].reset_index(drop=True),
                        "observed": self.y_test[
                            (self.y_test["distributor"] == area.distributor)
                            & (self.y_test["area_id"] == area.area_id)
                        ]["target"].reset_index(drop=True),
                        "predicted": self.trained_model.predict(
                            self.X_test[
                                (self.X_test["distributor"] == area.distributor)
                                & (self.X_test["area_id"] == area.area_id)
                            ][self.features]
                        ),
                    }
                )
                .sort_values("analysis_date")
                .reset_index(drop=True)
                for area in self.forecast_areas
            ]
        )
