"""Base config classes for pandera to standardize configuration defaults."""


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
