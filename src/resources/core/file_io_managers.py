import os
import json
from functools import partial
from typing import Callable, Union, IO, Any, Dict, Optional
from dagster import (
    AssetKey,
    OutputContext,
    InputContext,
    ConfigurableIOManager,
)


class FileIOManager(ConfigurableIOManager):
    """Base IO manager class for interacting with different file types and serialization protocols.

    Attributes:
        root_path: The root path for storing file artifacts.
        load_fn_kwargs: Keyword arguments to be supplied to the serialization load function.
        load_fn_kwargs: Keyword arguments to be supplied to the serialization dump function.
    """

    root_path: str
    load_fn_kwargs: Optional[Dict[str, Any]] = None
    dump_fn_kwargs: Optional[Dict[str, Any]] = None

    def _extension(self) -> str:
        raise NotImplementedError

    def _load_fn(self) -> Callable[[str], object]:
        raise NotImplementedError

    def _dump_fn(self) -> Callable[[object, Union[str, bytes]], None]:
        raise NotImplementedError

    def _get_path(self, asset_key: AssetKey, partition_key: str) -> str:
        return (
            os.path.join(self.root_path, "/".join(asset_key.path), partition_key)
            + f"{self._extension()}"
        )

    def handle_output(
        self, context: OutputContext, obj: object
    ) -> None:  # pragma: no cover
        """Required method for writing Dagster materialization outputs.

        Uses the _dump_fn to serialize the materialized asset result and writes the outputs to a file located at
        <root_path>/<asset_key>/<partition_key>.<extension>
        """
        dump_fn = partial(self._dump_fn(), **(self.dump_fn_kwargs or {}))
        output_file = self._get_path(context.asset_key, context.partition_key)
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, "w") as f:
            dump_fn(obj, f)

    def load_input(self, context: InputContext) -> object:  # pragma: no cover
        """Required method for loading upstream Dagster materialization results as inputs.

        Uses the _load_fn to deserialize the asset materialization from a file located at
        <root_path>/<asset_key>/<partition_key>.<extension>
        """
        load_fn = partial(self._load_fn(), **(self.load_fn_kwargs or {}))
        with open(self._get_path(context.asset_key, context.partition_key), "r") as f:
            return load_fn(f)


class JSONFileIOManager(FileIOManager):
    """IO manager class for writing and loading JSON asset materializations."""

    def _extension(self) -> str:
        return ".json"

    def _load_fn(self) -> Callable[[IO], object]:
        return json.load

    def _dump_fn(self) -> Callable[[object, IO], None]:
        return json.dump
