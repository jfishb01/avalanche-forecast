import pandera as pa

from src.schemas.schema_config import StrictDataFrameSchemaConfig


class AspectComponentSchema(pa.DataFrameModel):
    """Pandera schema for forecasting components of impacted aspects."""

    aspect_sin_alp_0: float = pa.Field(default=0.0)
    aspect_cos_alp_0: float = pa.Field(default=0.0)
    aspect_range_alp_0: float = pa.Field(default=0.0)
    aspect_sin_tln_0: float = pa.Field(default=0.0)
    aspect_cos_tln_0: float = pa.Field(default=0.0)
    aspect_range_tln_0: float = pa.Field(default=0.0)
    aspect_sin_btl_0: float = pa.Field(default=0.0)
    aspect_cos_btl_0: float = pa.Field(default=0.0)
    aspect_range_btl_0: float = pa.Field(default=0.0)
    aspect_sin_alp_1: float = pa.Field(default=0.0)
    aspect_cos_alp_1: float = pa.Field(default=0.0)
    aspect_range_alp_1: float = pa.Field(default=0.0)
    aspect_sin_tln_1: float = pa.Field(default=0.0)
    aspect_cos_tln_1: float = pa.Field(default=0.0)
    aspect_range_tln_1: float = pa.Field(default=0.0)
    aspect_sin_btl_1: float = pa.Field(default=0.0)
    aspect_cos_btl_1: float = pa.Field(default=0.0)
    aspect_range_btl_1: float = pa.Field(default=0.0)
    aspect_sin_alp_2: float = pa.Field(default=0.0)
    aspect_cos_alp_2: float = pa.Field(default=0.0)
    aspect_range_alp_2: float = pa.Field(default=0.0)
    aspect_sin_tln_2: float = pa.Field(default=0.0)
    aspect_cos_tln_2: float = pa.Field(default=0.0)
    aspect_range_tln_2: float = pa.Field(default=0.0)
    aspect_sin_btl_2: float = pa.Field(default=0.0)
    aspect_cos_btl_2: float = pa.Field(default=0.0)
    aspect_range_btl_2: float = pa.Field(default=0.0)

    class Config(metaclass=StrictDataFrameSchemaConfig):
        pass
