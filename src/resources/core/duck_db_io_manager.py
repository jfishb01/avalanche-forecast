from typing import Sequence, Union
import duckdb
from dagster import InputContext, OutputContext
from dagster_duckdb_pandas import DuckDBPandasIOManager as LibraryDuckDBPandasIOManager
from dagster_duckdb.io_manager import DuckDbClient

from dagster._core.storage.db_io_manager import (
    DbIOManager,
    TablePartitionDimension,
    TableSlice,
)


class DuckDBPandasIOManager(LibraryDuckDBPandasIOManager):
    """Augments the dagster supported DuckDBPandasIOManager by adding support for single run backfills.

    The built-in DuckDBPandasIOManager has a bug in which it cannot support single run backfills for
    MultiPartitionDefinitions. Details are included in this open bug:
    https://github.com/dagster-io/dagster/issues/19408
    """

    def create_io_manager(self, context) -> DbIOManager:
        return _DbIOManager(
            db_client=_DuckDbClient(),
            database=self.database,
            schema=self.schema_,
            type_handlers=self.type_handlers(),
            default_load_type=self.default_load_type(),
        )


class _DbIOManager(DbIOManager):
    """Internal class fixing the IO manager multi-partition bug."""

    def _get_table_slice(
        self, context: Union[OutputContext, InputContext], output_context: OutputContext
    ) -> TableSlice:
        """Overrides the buggy base class method and extracts the correct partition keys for iteration."""
        output_context_metadata = output_context.definition_metadata
        schema = output_context_metadata.get("schema")
        table = context.asset_key.path[-1]
        partition_dimensions = TablePartitionDimension(
            partition_expr=output_context_metadata.get("partition_expr"),
            partitions=context.asset_partition_keys,
        )
        return TableSlice(
            table=table.upper(),
            schema=schema.upper(),
            database=self._database.upper(),
            partition_dimensions=partition_dimensions,
            columns=(context.definition_metadata or {}).get("columns"),
        )


class _DuckDbClient(DuckDbClient):
    """Internal class fixing query bugs by building the correct queries for the provided TableSlice partitions."""

    @staticmethod
    def delete_table_slice(
        context: OutputContext, table_slice: TableSlice, connection
    ) -> None:
        try:
            query = (
                f"DELETE FROM {table_slice.schema}.{table_slice.table}\n"
                f"WHERE\n"
                f"{_partition_where_clause(table_slice.partition_dimensions)};"
            )
            connection.cursor().execute(query)
        except duckdb.CatalogException:
            # table doesn't exist yet, so ignore the error
            pass

    @staticmethod
    def get_select_statement(table_slice: TableSlice) -> str:
        col_str = ",\n\t".join(table_slice.columns) if table_slice.columns else "*"
        return (
            f"SELECT\n"
            f"  {col_str}\n"
            f"FROM {table_slice.schema}.{table_slice.table}\n"
            f"WHERE\n"
            f"{_partition_where_clause(table_slice.partition_dimensions)};"
        )


def _partition_where_clause(
    partition_dimensions: Sequence[TablePartitionDimension],
) -> str:
    # Build a sql WHERE clause using IN statements filtering on the sets of unique partition values
    partitions = {k: set() for k in partition_dimensions.partition_expr}
    for partition in partition_dimensions.partitions:
        for partition_key in partitions.keys():
            partitions[partition_key].add(partition.keys_by_dimension[partition_key])

    filters = {}
    for partition_key_name, table_column in partition_dimensions.partition_expr.items():
        filters[table_column] = ", ".join(
            [f"'{v}'" for v in partitions[partition_key_name]]
        )

    return " AND\n".join(
        [f"  {col} IN ({filter_str})" for col, filter_str in filters.items()]
    )
