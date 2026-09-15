import hashlib
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Annotated, Literal, Protocol

import polars as pl
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from udp.config.columns import WATERMARK_DECLARED_TYPES, DeclaredType
from udp.config.quality import Check
from udp.names import RESERVED_COLUMNS, name_problem
from udp.pipeline.transform import clean_column_names


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
    load_mode: Literal["full", "append", "merge"] = "full"
    watermark: str | None = Field(default=None, validate_default=True)
    primary_key: list[str] | None = Field(default=None, validate_default=True)
    columns: dict[str, DeclaredType] = {}
    checks: list[Check] = []
    quarantine_threshold_percent: float = Field(default=1, ge=0, le=100)

    @field_validator("columns")
    @classmethod
    def _columns_use_stored_names(
        cls, value: dict[str, str], info: ValidationInfo
    ) -> dict[str, str]:
        for name in value:
            (clean,) = clean_column_names([name])
            if name in RESERVED_COLUMNS:
                raise ValueError(f"'{name}' is a platform column")
            if clean != name:
                raise ValueError(
                    f"'{name}' is not a column name as stored; use the cleaned name '{clean}'"
                )
        watermark = info.data.get("watermark")
        declared = value.get(watermark) if watermark is not None else None
        if declared is not None and declared not in WATERMARK_DECLARED_TYPES:
            raise ValueError(
                f"watermark column '{watermark}' is declared {declared}; it must be "
                "integer, date or timestamp"
            )
        return value

    @field_validator("name")
    @classmethod
    def _name_is_identifier(cls, value: str) -> str:
        problem = name_problem(value)
        if problem is not None:
            raise ValueError(problem)
        return value

    @field_validator("watermark")
    @classmethod
    def _watermark_fits_load_mode(cls, value: str | None, info: ValidationInfo) -> str | None:
        mode = info.data.get("load_mode", "full")
        if mode == "full" and value is not None:
            raise ValueError("only used by append and merge")
        if mode != "full" and value is None:
            raise ValueError(f"required when load_mode is {mode}")
        if value in RESERVED_COLUMNS:
            raise ValueError(f"'{value}' is a platform column")
        return value

    @field_validator("primary_key")
    @classmethod
    def _primary_key_fits_load_mode(
        cls, value: list[str] | None, info: ValidationInfo
    ) -> list[str] | None:
        mode = info.data.get("load_mode", "full")
        if mode != "merge" and value is not None:
            raise ValueError("only used by merge")
        if mode == "merge":
            if not value:
                raise ValueError("required when load_mode is merge, with at least one column")
            if len(set(value)) != len(value):
                raise ValueError("lists a column more than once")
            reserved = [column for column in value if column in RESERVED_COLUMNS]
            if reserved:
                raise ValueError(f"'{reserved[0]}' is a platform column")
        return value


Watermark = int | date | datetime


@dataclass(frozen=True)
class SavedWatermark:
    """The highest watermark already loaded; inclusive re-reads rows equal to it."""

    column: str
    value: Watermark
    inclusive: bool


@dataclass(frozen=True)
class FileVersion:
    path: str
    sha256: str


def file_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


@dataclass(frozen=True)
class ExtractRequest[C: ConnectionBase, D: DatasetBase]:
    source_dir: Path
    connection: C
    dataset: D
    chunk_size: int
    watermark: SavedWatermark | None = None


class Connector[C: ConnectionBase, D: DatasetBase](Protocol):
    """Reads one dataset as chunks of at most chunk_size rows, all sharing one schema.

    A source problem raises ExtractError. A connector may use request.watermark to read
    less, but the pipeline filters rows either way.
    """

    @property
    def connection_model(self) -> type[C]: ...

    @property
    def dataset_model(self) -> type[D]: ...

    def file_version(self, request: ExtractRequest[C, D]) -> FileVersion | None:
        """The file and content hash a file source would read; None for other sources."""
        ...

    def extract(self, request: ExtractRequest[C, D]) -> Iterator[pl.DataFrame]: ...
