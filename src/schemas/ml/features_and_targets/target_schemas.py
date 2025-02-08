import pandera as pa
from dagster_pandera import pandera_schema_to_dagster_type

from src.schemas.schema_config import BaseMLSchema
from src.schemas.ml.features_and_targets.aspect_component_schemas import (
    AspectComponentSchema,
)


class TargetSchema(BaseMLSchema, AspectComponentSchema):
    """Pandera schema for ML model targets."""

    problem_0: int
    problem_1: int
    problem_2: int


TargetSchemaDagsterType = pandera_schema_to_dagster_type(TargetSchema)
