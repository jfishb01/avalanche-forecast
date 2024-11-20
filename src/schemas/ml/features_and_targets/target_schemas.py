from dagster_pandera import pandera_schema_to_dagster_type

from src.schemas.schema_config import BaseMLSchema


class TargetSchema(BaseMLSchema):
    """Pandera schema for ML model targets."""

    problem_0: int
    problem_1: int
    problem_2: int


TargetSchemaDagsterType = pandera_schema_to_dagster_type(TargetSchema)
