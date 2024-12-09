import pandas as pd
import pandera as pa


def conform_to_schema(
    df: pd.DataFrame, schema_model: pa.api.base.model.MetaModel
) -> pd.DataFrame:
    """Match the dataframe to the schema by filling missing columns with defaults and removing extra columns."""
    schema_columns = schema_model.to_schema().columns
    missing_columns = [col for col in schema_columns.keys() if col not in df.columns]
    standardized_df = df.assign(**{col: None for col in missing_columns})[
        schema_columns.keys()
    ]
    for col_name, col_definition in schema_columns.items():
        if not col_definition.nullable and col_definition.default is not None:
            standardized_df[col_name] = standardized_df[col_name].fillna(
                col_definition.default
            )
    return standardized_df
