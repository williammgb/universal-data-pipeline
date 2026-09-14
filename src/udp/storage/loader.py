from collections.abc import Iterable, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal, Protocol
from uuid import UUID

import polars as pl

from udp.errors import LoadError, SchemaDriftError

Column = tuple[str, str]
Watermark = int | date | datetime

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

WATERMARK_TYPES = frozenset(
    {
        "smallint",
        "integer",
        "bigint",
        "date",
        "timestamp without time zone",
        "timestamp with time zone",
    }
)


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


@dataclass(frozen=True)
class ColumnChanges:
    added: tuple[Column, ...]
    missing: tuple[str, ...]


def column_changes(table: str, existing: Sequence[Column], incoming: pl.Schema) -> ColumnChanges:
    """How a table's columns change to take a batch. Columns are only ever added.

    A column the batch lacks is kept; a column with no values in the batch fits any
    existing type; the same column arriving with another type raises SchemaDriftError.
    """
    stored = dict(existing)
    added = []
    for name, dtype in incoming.items():
        kind = column_type(name, dtype)
        if name not in stored:
            added.append((name, kind))
        elif kind != stored[name] and not isinstance(dtype, pl.Null):
            raise SchemaDriftError(
                f"column '{name}' of datasets.{table} is {stored[name]} but this run read it "
                f"as {kind}; run with --full-refresh to rebuild the table"
            )
    missing = tuple(name for name in stored if name not in incoming)
    return ColumnChanges(tuple(added), missing)


@dataclass(frozen=True)
class LoadResult:
    rows: int
    added: tuple[Column, ...] = ()
    missing: tuple[str, ...] = ()


@dataclass(frozen=True)
class DatasetState:
    """What was loaded last: where to continue from and what the load depended on."""

    source: str
    dataset: str
    load_mode: str
    primary_key: tuple[str, ...]
    watermark_column: str | None
    watermark_type: str | None
    watermark: Watermark | None
    file_path: str | None
    file_sha256: str | None
    config_sha256: str
    run_id: UUID
    saved_at: datetime


def watermark_to_text(value: Watermark) -> str:
    return str(value) if isinstance(value, int) else value.isoformat()


def watermark_from_text(text: str, kind: str) -> Watermark:
    if kind in ("smallint", "integer", "bigint"):
        return int(text)
    if kind == "date":
        return date.fromisoformat(text)
    return datetime.fromisoformat(text)


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
    def read_state(self, source: str, dataset: str) -> DatasetState | None: ...

    def save_state(self, state: DatasetState) -> None: ...

    def drop_table(self, table: str) -> None:
        """Remove datasets.<table> if it exists."""
        ...

    def replace_table(self, table: str, chunks: Iterable[pl.DataFrame]) -> LoadResult:
        """Replace every row of datasets.<table> with the chunks."""
        ...

    def append_rows(self, table: str, chunks: Iterable[pl.DataFrame]) -> LoadResult:
        """Add the chunks' rows to datasets.<table>."""
        ...

    def merge_rows(
        self,
        table: str,
        chunks: Iterable[pl.DataFrame],
        *,
        primary_key: Sequence[str],
        watermark: str,
    ) -> LoadResult:
        """Insert new keys and update rows whose content changed; unchanged rows stay untouched.

        Within the chunks the row with the highest watermark wins, the last one on a tie.
        """
        ...

    def record_columns(
        self, table: str, source: str, dataset: str, run_id: UUID, recorded_at: datetime
    ) -> int | None:
        """Add a schema version when the table's columns differ from the last one recorded."""
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
