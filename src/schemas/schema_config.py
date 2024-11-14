"""Base config classes for pandera to standardize configuration defaults."""

import pytz
import pandera as pa
import pandas as pd
from datetime import datetime


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
    run_id: str
    run_key: str
    creation_datetime: pd.DatetimeTZDtype = pa.Field(
        dtype_kwargs={"unit": "ms", "tz": "UTC"}, default=datetime.now(pytz.UTC)
    )
    publish_datetime: pd.DatetimeTZDtype = pa.Field(
        dtype_kwargs={"unit": "ms", "tz": "UTC"}
    )

    class Config(metaclass=StrictDataFrameSchemaConfig):
        pass
