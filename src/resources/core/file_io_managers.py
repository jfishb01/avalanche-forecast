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

    def handle_output(self, context: OutputContext, obj: object) -> None:
        dump_fn = partial(self._dump_fn(), **(self.dump_fn_kwargs or {}))
        output_file = self._get_path(context.asset_key, context.partition_key)
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, "w") as f:
            dump_fn(obj, f)

    def load_input(self, context: InputContext) -> object:
        load_fn = partial(self._load_fn(), **(self.load_fn_kwargs or {}))
        with open(self._get_path(context.asset_key, context.partition_key), "r") as f:
            return load_fn(f)


class JSONFileIOManager(FileIOManager):
    def _extension(self) -> str:
        return ".json"

    def _load_fn(self) -> Callable[[IO], object]:
        return json.load

    def _dump_fn(self) -> Callable[[object, IO], None]:
        return json.dump
