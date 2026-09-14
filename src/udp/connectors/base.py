from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Annotated, Literal, Protocol

import polars as pl
from pydantic import AfterValidator, BaseModel, ConfigDict, field_validator

from udp.names import name_problem


def _inside_source_folder(value: str) -> str:
    path = PureWindowsPath(value)
    if path.drive or path.root or PurePosixPath(value).is_absolute():
        raise ValueError("must be a relative path inside the source folder")
    if ".." in path.parts:
        raise ValueError("must not contain '..'")
    return value


SourcePath = Annotated[str, AfterValidator(_inside_source_folder)]


class ConnectionBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str


class DatasetBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    load_mode: Literal["full"] = "full"

    @field_validator("name")
    @classmethod
    def _name_is_identifier(cls, value: str) -> str:
        problem = name_problem(value)
        if problem is not None:
            raise ValueError(problem)
        return value


@dataclass(frozen=True)
class ExtractRequest[C: ConnectionBase, D: DatasetBase]:
    source_dir: Path
    connection: C
    dataset: D
    chunk_size: int


class Connector[C: ConnectionBase, D: DatasetBase](Protocol):
    """Reads one dataset as chunks of at most chunk_size rows, all sharing one schema.

    A source problem raises ExtractError.
    """

    @property
    def connection_model(self) -> type[C]: ...

    @property
    def dataset_model(self) -> type[D]: ...

    def extract(self, request: ExtractRequest[C, D]) -> Iterator[pl.DataFrame]: ...
