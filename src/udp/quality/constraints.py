"""Checking a dataset's constraints: what it should look like, without changing a single value.

`check_frame` checks a Polars frame; `check_stage` checks the dataset's RAW, STAGING or CLEAN
table, and `PostgresStages.check_constraints` stores what it returns, tagged with the dataset,
the stage and the run. Each outcome counts every row and every value that broke its constraint,
and keeps the first MAX_VIOLATIONS of those values as violation records naming the column, the
row and the value.

How a large table is bounded: it is read READ_BATCH rows at a time — every row, never a sample —
and only the columns the constraints and the row key need, so one batch is held at a time; a
unique constraint is counted by PostgreSQL with GROUP BY, so the whole table is never held at
once.
"""

import json
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, cast

import polars as pl
import psycopg
from psycopg import sql
from psycopg.rows import tuple_row

from udp.config.constraints import Constraint, Datatype, UniqueConstraint, to_check
from udp.connectors.base import DatasetBase
from udp.errors import LoadError, ValidationError
from udp.names import Stage, stage_table
from udp.pipeline.column_types import check_declared_columns, convert, unfit_values
from udp.profiling.models import json_value
from udp.profiling.stage import read_as, stage_columns
from udp.quality.checks import RowCheck, check_columns, row_fails
from udp.storage.loader import ConstraintResult, RunRef, Violation

MAX_VIOLATIONS = 100
READ_BATCH = 50_000
# The column a frame's rows are numbered in when it has no key to name a row by.
_POSITION = "__udp_row"


@dataclass(frozen=True)
class Outcome:
    """One constraint's outcome: how many rows and values broke it, and the first of them."""

    position: int
    constraint: Constraint
    failing_rows: int
    failing_values: int
    violations: tuple[Violation, ...]

    @property
    def passed(self) -> bool:
        return self.failing_rows == 0

    @property
    def message(self) -> str:
        if self.passed:
            return "every row holds"
        return f"{self.failing_rows} rows and {self.failing_values} values break it"

    def result(
        self,
        source: str,
        dataset: str,
        stage: Stage,
        run: RunRef,
        checked_at: datetime,
        after_step: int | None = None,
    ) -> ConstraintResult:
        return ConstraintResult(
            source=source,
            dataset=dataset,
            stage=stage,
            run=run,
            position=self.position,
            constraint_type=self.constraint.constraint,
            columns=tuple(self.constraint.columns_checked),
            critical=self.constraint.critical,
            passed=self.passed,
            failing_rows=self.failing_rows,
            failing_values=self.failing_values,
            message=self.message,
            settings=self.constraint.model_dump(mode="json", exclude={"constraint", "critical"}),
            checked_at=checked_at,
            after_step=after_step,
            violations=self.violations,
        )


def _text(value: Any) -> str | None:
    """A broken value as the text a violation record holds."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, dict):
        return json.dumps(value)
    return str(json_value(value))


@dataclass
class _Tally:
    rows: int = 0
    values: int = 0
    violations: list[Violation] = field(default_factory=list)

    def add(self, rows: int, columns: int, broken: Iterable[Violation]) -> None:
        self.rows += rows
        self.values += rows * columns
        for violation in broken:
            if len(self.violations) >= MAX_VIOLATIONS:
                break
            self.violations.append(violation)

    def outcome(self, position: int, constraint: Constraint) -> Outcome:
        return Outcome(position, constraint, self.rows, self.values, tuple(self.violations))


def row_key_columns(columns: Iterable[str], primary_key: Sequence[str] | None) -> list[str]:
    """The columns a violation names its row by: the primary key, else the record hash, else none
    (the row's position)."""
    present = set(columns)
    if primary_key and all(name in present for name in primary_key):
        return list(primary_key)
    return ["_record_hash"] if "_record_hash" in present else []


def _label(position: int, constraint: Constraint) -> str:
    return f"constraint {position} ({constraint.constraint})"


def _check_columns(schema: pl.Schema, constraints: Sequence[Constraint]) -> None:
    """Every constraint names columns that exist, of a type it can compare — V1's rules."""
    for position, constraint in enumerate(constraints, 1):
        for name in constraint.columns_checked:
            if name not in schema:
                raise ValidationError(
                    f"{_label(position, constraint)}: column '{name}' is not in the data"
                )
    checks = [
        (position, constraint, check)
        for position, constraint in enumerate(constraints, 1)
        if (check := to_check(constraint)) is not None
    ]
    check_columns(
        schema,
        [check for _, _, check in checks],
        [_label(position, constraint) for position, constraint, _ in checks],
    )


def _as_declared(series: pl.Series, declared: str) -> pl.Series:
    """The column in the declared type, null where a value does not fit — every value, when the
    column holds a type that can never become the declared one (as the profile counts them)."""
    try:
        check_declared_columns(pl.Schema({series.name: series.dtype}), {series.name: declared})
    except ValidationError:
        return pl.Series(series.name, [None] * series.len(), dtype=pl.Null)
    return convert(series, declared)


def _breaks(frame: pl.DataFrame, constraint: Constraint) -> pl.Series:
    """True for each row that breaks the constraint; nulls break only not_null."""
    if isinstance(constraint, UniqueConstraint):
        columns = constraint.columns
        present = pl.all_horizontal(pl.col(name).is_not_null() for name in columns)
        return frame.select(present & (pl.len().over(columns) > 1)).to_series()
    if isinstance(constraint, Datatype):
        series = frame[constraint.column]
        return unfit_values(series, _as_declared(series, constraint.type))
    check = cast(RowCheck, to_check(constraint))
    return frame.select(row_fails(check, frame.schema[check.column]).fill_null(False)).to_series()


def _violations(
    broken: pl.DataFrame, constraint: Constraint, key: Sequence[str]
) -> Iterator[Violation]:
    for row in broken.iter_rows(named=True):
        row_key = {name: json_value(row[name]) for name in key} if key else {"row": row[_POSITION]}
        for column in constraint.columns_checked:
            yield Violation(column, row_key, _text(row[column]))


def _tally_batch(
    frame: pl.DataFrame,
    constraints: Sequence[Constraint],
    tallies: Sequence[_Tally],
    key: Sequence[str],
    offset: int,
    *,
    unique: bool,
) -> None:
    numbered = frame.with_row_index(_POSITION, offset=offset + 1)
    for constraint, tally in zip(constraints, tallies, strict=True):
        if isinstance(constraint, UniqueConstraint) and not unique:
            continue
        broken = numbered.filter(_breaks(frame, constraint))
        room = MAX_VIOLATIONS - len(tally.violations)
        columns = len(constraint.columns_checked)
        shown = broken.head(-(-room // columns)) if room > 0 else broken.clear()
        tally.add(broken.height, columns, _violations(shown, constraint, key))


def check_frame(
    frame: pl.DataFrame,
    constraints: Sequence[Constraint],
    primary_key: Sequence[str] | None = None,
) -> list[Outcome]:
    """Check every constraint against the frame, which is only read.

    Raises ValidationError when a constraint names a column that is not there, or one of a type
    it cannot compare with.
    """
    _check_columns(frame.schema, constraints)
    tallies = [_Tally() for _ in constraints]
    key = row_key_columns(frame.columns, primary_key)
    _tally_batch(frame, constraints, tallies, key, 0, unique=True)
    return [
        tally.outcome(position, constraint)
        for position, (constraint, tally) in enumerate(zip(constraints, tallies, strict=True), 1)
    ]


def _frame_type(name: str, stored_type: str) -> tuple[sql.Composable, pl.DataType]:
    """How a stage column is selected and its type in the frame: as the profile reads it, with a
    numeric too wide for Polars' decimals a float."""
    expression, dtype = read_as(name, stored_type)
    if stored_type.startswith("numeric") and isinstance(dtype, pl.String):
        return sql.SQL("{}::double precision").format(sql.Identifier(name)), pl.Float64()
    return expression, dtype


def _batches(
    conn: psycopg.Connection[Any], query: sql.Composed, schema: pl.Schema
) -> Iterator[pl.DataFrame]:
    with conn.cursor(name="udp_constraint_read", row_factory=tuple_row) as cursor:
        cursor.itersize = READ_BATCH
        cursor.execute(query)
        while rows := cursor.fetchmany(READ_BATCH):
            yield pl.DataFrame(rows, schema=schema, orient="row")


def _unique_in_table(
    conn: psycopg.Connection[Any],
    table: sql.Identifier,
    constraint: UniqueConstraint,
    key: Sequence[str],
    tally: _Tally,
) -> None:
    """Count the rows sharing their unique columns with another row, inside PostgreSQL."""
    columns = constraint.columns
    names = sql.SQL(", ").join(sql.Identifier(name) for name in columns)
    present = sql.SQL(" AND ").join(
        sql.SQL("{} IS NOT NULL").format(sql.Identifier(name)) for name in columns
    )
    shared = sql.SQL(
        "({names}) IN (SELECT {names} FROM {table} WHERE {present} "
        "GROUP BY {names} HAVING count(*) > 1)"
    ).format(names=names, table=table, present=present)
    row = conn.execute(sql.SQL("SELECT count(*) FROM {} WHERE {}").format(table, shared)).fetchone()
    rows = int(row[0]) if row else 0
    keys: list[sql.Composable] = [sql.Identifier(name) for name in key] or [sql.SQL("ctid::text")]
    texts = [sql.SQL("{}::text").format(sql.Identifier(name)) for name in columns]
    found = conn.execute(
        sql.SQL("SELECT {} FROM {} WHERE {} ORDER BY {} LIMIT %s").format(
            sql.SQL(", ").join([*keys, *texts]), table, shared, names
        ),
        [-(-MAX_VIOLATIONS // len(columns))],
    ).fetchall()
    labels = list(key) or ["row"]
    tally.add(
        rows,
        len(columns),
        (
            Violation(
                column,
                {label: json_value(value) for label, value in zip(labels, found_row, strict=False)},
                text,
            )
            for found_row in found
            for column, text in zip(columns, found_row[len(labels) :], strict=True)
        ),
    )


def check_stage(
    conn: psycopg.Connection[Any], stage: Stage, source: str, dataset: DatasetBase
) -> list[Outcome]:
    """Check the dataset's constraints against its table at this stage, which is only read.

    Raises LoadError when there is no such table, and ValidationError as `check_frame` does.
    """
    schema_name, table_name = stage_table(stage, source, dataset.name)
    table = sql.Identifier(schema_name, table_name)
    constraints = dataset.constraints
    with conn.transaction():
        types = stage_columns(conn, schema_name, table_name)
        if not types:
            raise LoadError(f"{source}.{dataset.name} has no {Stage(stage).value} table")
        reads = {name: _frame_type(name, kind) for name, kind in types}
        _check_columns(pl.Schema({name: dtype for name, (_, dtype) in reads.items()}), constraints)
        if not constraints:
            return []
        key = row_key_columns(reads, dataset.primary_key)
        needed = {*key, *(name for c in constraints for name in c.columns_checked)}
        chosen = [name for name in reads if name in needed]
        frame_schema = pl.Schema({name: reads[name][1] for name in chosen})
        query = sql.SQL("SELECT {} FROM {}").format(
            sql.SQL(", ").join(reads[name][0] for name in chosen), table
        )
        tallies = [_Tally() for _ in constraints]
        offset = 0
        for batch in _batches(conn, query, frame_schema):
            _tally_batch(batch, constraints, tallies, key, offset, unique=False)
            offset += batch.height
        for constraint, tally in zip(constraints, tallies, strict=True):
            if isinstance(constraint, UniqueConstraint):
                _unique_in_table(conn, table, constraint, key, tally)
    return [
        tally.outcome(position, constraint)
        for position, (constraint, tally) in enumerate(zip(constraints, tallies, strict=True), 1)
    ]
