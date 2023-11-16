from datetime import date
from enum import StrEnum, auto
from pydantic import BaseModel


class ForecastDistributorEnum(StrEnum):
    CAIC = auto()
    FAC = auto()


class RawAvalancheForecast(BaseModel):
    analysis_date: date
    forecast: str
