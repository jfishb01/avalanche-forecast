"""Base config classes for pandera to standardize configuration defaults."""

import pytz
import pandera as pa
import pandas as pd
from datetime import datetime, date


class StrictDataFrameSchemaConfig(type):
    def __new__(cls, name, bases, dct):
        dct["strict"] = True
        dct["coerce"] = True
        dct["unique_column_names"] = True
        return super().__new__(cls, name, bases, dct)


class FlexibleDataFrameSchemaConfig(type):
    def __new__(cls, name, bases, dct):
        dct["strict"] = False
        dct["coerce"] = True
        dct["unique_column_names"] = True
        return super().__new__(cls, name, bases, dct)


class BaseAssetSchema(pa.DataFrameModel):
    # Base schema with metadata columns that should be inherited from for every asset materialization
    run_id: str
    run_key: str
    creation_datetime: pd.DatetimeTZDtype = pa.Field(
        dtype_kwargs={"unit": "ms", "tz": "UTC"}, default=datetime.now(pytz.UTC)
    )

    class Config(metaclass=StrictDataFrameSchemaConfig):
        pass


class BaseMLSchema(BaseAssetSchema):
    # Base schema with required partition columns that should be included for all ML related assets.
    forecast_date: date
    region_id: str
