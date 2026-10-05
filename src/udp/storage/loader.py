from collections.abc import Iterable, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Literal, Protocol
from uuid import UUID

import polars as pl

from udp.config.columns import is_json
from udp.errors import LoadError, SchemaDriftError
from udp.names import Stage, stage_table

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
    if is_json(dtype):
        return "jsonb"
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


def column_changes(
    table: str, existing: Sequence[Column], incoming: pl.Schema, schema: str = "datasets"
) -> ColumnChanges:
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
            remedy = (
                "run with --full-refresh to rebuild the table"
                if schema == "datasets"
                else "RAW keeps what was ingested, so a column never changes type"
            )
            raise SchemaDriftError(
                f"column '{name}' of {schema}.{table} is {stored[name]} but this run read it "
                f"as {kind}; {remedy}"
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

    def drop_raw(self, source: str, dataset: str) -> None:
        """Remove the dataset's RAW table if it exists, so the next load starts it over."""
        ...

    def append_raw(self, source: str, dataset: str) -> int:
        """Add the rows the last load in this transaction read, platform columns included, to the
        dataset's RAW table, creating it on first use. Returns the rows added."""
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

    def read_overrides(self, source: str) -> dict[str, dict[str, Any]]:
        """Each of the source's datasets' stored configuration edits, by dataset name."""
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


# V2: what pipelines are, what each run of one did, and what was measured at each stage.
# The tables are created by migration 0006 and described in docs/stages-and-pipelines.md.


@dataclass(frozen=True)
class RunRef:
    """The run a stored result belongs to: an ingest run (`platform.pipeline_runs`) or a run of a
    V2 pipeline (`platform.pipeline_executions`) — exactly one of the two."""

    ingest_run_id: UUID | None = None
    execution_id: UUID | None = None

    def __post_init__(self) -> None:
        if (self.ingest_run_id is None) == (self.execution_id is None):
            raise ValueError("a result belongs to exactly one run: an ingest run or an execution")


def _check_after_step(stage: Stage, run: RunRef, after_step: int | None) -> None:
    """A result taken between steps is of STAGING, inside an execution, after step 1 or later."""
    if after_step is None:
        return
    if after_step < 1:
        raise ValueError(f"after_step must be 1 or more, not {after_step}")
    if stage is not Stage.STAGING or run.execution_id is None:
        raise ValueError("only a STAGING result inside a pipeline execution follows a step")


@dataclass(frozen=True)
class StepDefinition:
    """One step of a pipeline, in its place in the list, with the settings it runs with."""

    step_type: str
    configuration: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.step_type:
            raise ValueError("a step needs a type")


@dataclass(frozen=True)
class PipelineVersion:
    """One version of a pipeline definition. Versions are never changed: an edit adds one."""

    pipeline_id: int
    source: str
    dataset: str
    name: str
    version: int
    definition: dict[str, Any]
    steps: tuple[StepDefinition, ...]
    created_at: datetime


@dataclass(frozen=True)
class ExecutionStart:
    execution_id: UUID
    pipeline_id: int
    version: int
    trigger: Literal["manual", "scheduled"]
    started_at: datetime


StepStatus = Literal["running", "succeeded", "failed"]


@dataclass(frozen=True)
class StepRun:
    """What one step did in one execution; `position` is the step's place in the pipeline. A
    python step also records the hash of the script it ran, what the script printed, and the
    script line it failed on."""

    position: int
    status: StepStatus
    started_at: datetime
    ended_at: datetime | None = None
    rows_in: int | None = None
    rows_out: int | None = None
    values_changed: int | None = None
    error_class: str | None = None
    error_message: str | None = None
    script_sha256: str | None = None
    output: str | None = None
    error_line: int | None = None

    def __post_init__(self) -> None:
        if self.position < 1:
            raise ValueError(f"a step's position is 1 or more, not {self.position}")
        if (self.status == "running") != (self.ended_at is None):
            raise ValueError("a step has an end time exactly when it is no longer running")
        if (self.status == "failed") != (self.error_message is not None):
            raise ValueError("a step has an error message exactly when it failed")


@dataclass(frozen=True)
class Execution:
    """One run of one pipeline version, as stored, with its steps in order."""

    execution_id: UUID
    pipeline_id: int
    version: int
    source: str
    dataset: str
    trigger: str
    status: str
    started_at: datetime
    ended_at: datetime | None
    rows_in: int | None
    rows_out: int | None
    failed_step: int | None
    error_class: str | None
    error_message: str | None
    steps: tuple[StepRun, ...]


@dataclass(frozen=True)
class Profile:
    """A profile of one dataset at one stage, taken in one run; `result` is the profile itself."""

    source: str
    dataset: str
    stage: Stage
    run: RunRef
    table_rows: int
    result: dict[str, Any]
    profiled_at: datetime
    after_step: int | None = None

    def __post_init__(self) -> None:
        stage_table(self.stage, self.source, self.dataset)
        _check_after_step(Stage(self.stage), self.run, self.after_step)
        if self.table_rows < 0:
            raise ValueError(f"a table has 0 rows or more, not {self.table_rows}")


@dataclass(frozen=True)
class StoredProfile:
    profile_id: int
    profile: Profile


@dataclass(frozen=True)
class Violation:
    """One value that broke a constraint, and the row it is in, by its key or its position."""

    column: str
    row_key: dict[str, Any]
    value: str | None


@dataclass(frozen=True)
class ConstraintResult:
    """One constraint's outcome on one dataset at one stage in one run."""

    source: str
    dataset: str
    stage: Stage
    run: RunRef
    position: int
    constraint_type: str
    columns: tuple[str, ...]
    critical: bool
    passed: bool
    failing_rows: int | None
    failing_values: int | None
    message: str
    settings: dict[str, Any]
    checked_at: datetime
    after_step: int | None = None
    violations: tuple[Violation, ...] = ()

    def __post_init__(self) -> None:
        stage_table(self.stage, self.source, self.dataset)
        _check_after_step(Stage(self.stage), self.run, self.after_step)
        if self.position < 1:
            raise ValueError(f"a constraint's position is 1 or more, not {self.position}")
        for count in (self.failing_rows, self.failing_values):
            if count is not None and count < 0:
                raise ValueError(f"a count of failures is 0 or more, not {count}")
        if self.passed and self.violations:
            raise ValueError("a constraint that held has no violations")


NodeKind = Literal["source", "raw", "step", "clean"]


@dataclass(frozen=True)
class LineageNode:
    """One place data passed through. `name` is where the source was read from for a source,
    the schema-qualified table for RAW and CLEAN, and the step's type for a step."""

    kind: NodeKind
    name: str
    step_position: int | None = None

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("a lineage node needs a name")
        if (self.kind == "step") != (self.step_position is not None):
            raise ValueError("a step node, and only a step node, has a step position")
        if self.step_position is not None and self.step_position < 1:
            raise ValueError(f"a step's position is 1 or more, not {self.step_position}")


def stage_node(stage: Stage, source: str, dataset: str) -> LineageNode:
    """The lineage node of a dataset's RAW or CLEAN table."""
    if stage is Stage.STAGING:
        raise ValueError("STAGING is not in the lineage: the steps are")
    schema, table = stage_table(stage, source, dataset)
    return LineageNode("raw" if stage is Stage.RAW else "clean", f"{schema}.{table}")


def check_chain(source: str, dataset: str, run: RunRef, nodes: Sequence[LineageNode]) -> None:
    """Raise ValueError unless the nodes are, in order, the start of a valid chain for the run.

    An ingest run takes a source to RAW. An execution takes RAW through its steps, in pipeline
    order, to CLEAN; a chain that stops early is one recorded as the run goes, or a run that failed.
    """
    allowed: dict[str | None, set[str]] = (
        {None: {"source"}, "source": {"raw"}}
        if run.ingest_run_id is not None
        else {None: {"raw"}, "raw": {"step", "clean"}, "step": {"step", "clean"}}
    )
    expected = {
        "raw": stage_node(Stage.RAW, source, dataset).name,
        "clean": stage_node(Stage.CLEAN, source, dataset).name,
    }
    previous: LineageNode | None = None
    for node in nodes:
        if node.kind not in allowed.get(previous.kind if previous else None, set()):
            after = f"after {previous.kind}" if previous else "first"
            raise ValueError(f"a {node.kind} node cannot come {after} in this run's lineage")
        if node.kind in expected and node.name != expected[node.kind]:
            raise ValueError(f"the {node.kind} node of {source}.{dataset} is {expected[node.kind]}")
        if (
            node.step_position is not None
            and previous is not None
            and previous.step_position is not None
            and node.step_position <= previous.step_position
        ):
            raise ValueError("steps appear in the lineage in pipeline order")
        previous = node
