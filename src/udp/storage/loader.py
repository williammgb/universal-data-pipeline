from collections.abc import Iterable, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Literal, Protocol
from uuid import UUID

import polars as pl

from udp.errors import LoadError, SchemaDriftError

Column = tuple[str, str]
Watermark = int | date | datetime

INTERRUPTED = "Interrupted"

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
    if isinstance(dtype, pl.Decimal):
        return f"numeric({dtype.precision},{dtype.scale})"
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


@dataclass(frozen=True)
class CheckResult:
    """One quality check's outcome in one run; position is its place in the dataset's checks."""

    position: int
    check_type: str
    columns: tuple[str, ...]
    severity: str
    passed: bool
    failing_rows: int | None
    table_rows: int | None
    message: str
    settings: dict[str, Any]


@dataclass
class RunFindings:
    """What a run quarantined and what its checks found, recorded whether it succeeds or not.

    `quarantine` holds frames with a `reason` and a `record` (the row as JSON text); it keeps
    at most a capped number of rows, while `quarantined_rows` is the exact count.
    """

    source: str
    dataset: str
    quarantined_rows: int = 0
    quarantine: list[pl.DataFrame] = field(default_factory=list)
    results: list[CheckResult] = field(default_factory=list)


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

    def table_rows(self, table: str) -> int: ...

    def duplicate_rows(self, table: str, columns: Sequence[str]) -> int:
        """Rows sharing their values in `columns` with another row; rows with a null there
        are not counted."""
        ...

    def newest_value(self, table: str, column: str) -> date | datetime | None: ...

    def previous_table_rows(self, source: str, dataset: str) -> int | None:
        """The table's row count most recently recorded by a succeeded run's checks."""
        ...

    def record_findings(self, run_id: UUID, findings: RunFindings, recorded_at: datetime) -> None:
        """Write quarantined rows and check results, and the run's quarantined count."""
        ...

    def succeed_run(
        self, run_id: UUID, *, ended_at: datetime, rows_extracted: int, rows_loaded: int
    ) -> None: ...


@dataclass(frozen=True)
class ConfigCopy:
    """A source's connection and one dataset's settings as written, copied by a run."""

    source: str
    connector_type: str
    connection: dict[str, Any]
    dataset: str
    table: str
    load_mode: str
    primary_key: tuple[str, ...]
    watermark: str | None
    schedule: str | None
    definition: dict[str, Any]
    run_id: UUID
    recorded_at: datetime


def interrupted_message(found_by: UUID) -> str:
    return f"the run stopped without finishing; found when run {found_by} started"


class Loader(Protocol):
    def lock_dataset(self, source: str, dataset: str) -> bool:
        """Take the dataset's run lock without waiting; False when any run holds it already,
        including a run on this same loader. A lock held by a dead process is freed with it."""
        ...

    def unlock_dataset(self, source: str, dataset: str) -> None:
        """Release the dataset's run lock. Never raises: a lock that cannot be released is
        logged, and it is freed when the connection closes."""
        ...

    def skip_run(self, run: RunStart, *, ended_at: datetime) -> None:
        """Record a run that did not start because another run held the lock. Commits
        immediately."""
        ...

    def fail_interrupted_runs(
        self, source: str, dataset: str, *, found_by: UUID, ended_at: datetime
    ) -> int:
        """Mark the dataset's `running` runs failed with error class Interrupted; call only
        while holding the dataset's lock. Commits immediately and returns how many."""
        ...

    def start_run(self, run: RunStart) -> None:
        """Record a run as running. Commits immediately."""
        ...

    def record_config(self, copy: ConfigCopy) -> None:
        """Replace the stored copy of the source's connection and of this dataset's settings.
        Other datasets' copies are left as they are. Commits immediately."""
        ...

    def transaction(self) -> AbstractContextManager[LoadTransaction]:
        """Everything done through the transaction commits together, or not at all."""
        ...

    def fail_run(
        self,
        run: RunStart,
        *,
        ended_at: datetime,
        rows_extracted: int | None,
        failure: RunFailure,
    ) -> None:
        """Record a run as failed, writing its row if the run never got one. Commits at once."""
        ...
