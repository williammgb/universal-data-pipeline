from collections.abc import Iterable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol
from uuid import UUID

import polars as pl

from udp.errors import LoadError

Column = tuple[str, str]

_PLATFORM_COLUMN_TYPES = {
    "_run_id": "uuid",
    "_loaded_at": "timestamp with time zone",
    "_record_hash": "text",
}

_SIMPLE_TYPES: dict[type[pl.DataType], str] = {
    pl.Int8: "smallint",
    pl.Int16: "smallint",
    pl.Int32: "integer",
    pl.Int64: "bigint",
    pl.Float32: "real",
    pl.Float64: "double precision",
    pl.Boolean: "boolean",
    pl.String: "text",
    pl.Null: "text",
    pl.Date: "date",
    pl.Time: "time without time zone",
}


def column_type(name: str, dtype: pl.DataType) -> str:
    """The Postgres type a column is stored as, spelled the way format_type() reports it."""
    if name in _PLATFORM_COLUMN_TYPES:
        return _PLATFORM_COLUMN_TYPES[name]
    if isinstance(dtype, pl.Datetime):
        return "timestamp with time zone" if dtype.time_zone else "timestamp without time zone"
    simple = _SIMPLE_TYPES.get(dtype.base_type())
    if simple is None:
        raise LoadError(f"column '{name}' has type {dtype}, which cannot be stored yet")
    return simple


def table_columns(frame: pl.DataFrame) -> list[Column]:
    return [(name, column_type(name, dtype)) for name, dtype in frame.schema.items()]


def columns_mismatch(table: str, existing: list[Column], incoming: list[Column]) -> LoadError:
    return LoadError(
        f"table datasets.{table} has columns {existing}, but this run produced {incoming}"
    )


@dataclass(frozen=True)
class RunStart:
    run_id: UUID
    source: str
    dataset: str
    trigger: Literal["manual", "scheduled"]
    started_at: datetime


@dataclass(frozen=True)
class RunFailure:
    error_class: str
    message: str
    traceback: str


class LoadTransaction(Protocol):
    def replace_table(self, table: str, chunks: Iterable[pl.DataFrame]) -> int:
        """Replace every row of datasets.<table> with the chunks; returns rows loaded."""
        ...

    def succeed_run(
        self, run_id: UUID, *, ended_at: datetime, rows_extracted: int, rows_loaded: int
    ) -> None: ...


class Loader(Protocol):
    def start_run(self, run: RunStart) -> None:
        """Record a run as running. Commits immediately."""
        ...

    def transaction(self) -> AbstractContextManager[LoadTransaction]:
        """Everything done through the transaction commits together, or not at all."""
        ...

    def fail_run(
        self,
        run_id: UUID,
        *,
        ended_at: datetime,
        rows_extracted: int | None,
        failure: RunFailure,
    ) -> None:
        """Record a run as failed. Commits immediately."""
        ...
