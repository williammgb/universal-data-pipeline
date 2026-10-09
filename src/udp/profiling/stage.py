"""Profiling a dataset at a stage: its RAW, STAGING or CLEAN table, read into a frame.

A table over the row limit is read as a random sample of that many rows, drawn exactly as the
dashboard's profile draws one, and its profile says so. `PostgresStages.profile` stores what this
returns, tagged with the dataset, the stage and the run.
"""

import re
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import polars as pl
import psycopg
from psycopg import sql
from psycopg.rows import tuple_row

from udp.errors import LoadError
from udp.names import Stage, stage_table
from udp.profiling.frame import ProfileSettings, profile_frame
from udp.profiling.models import StageProfile
from udp.profiling.table import PROFILE_ROW_LIMIT, sample_rows

# Rows are read from the database this many at a time, each batch becoming a frame at once, so
# no more than one batch of rows is ever held as Python objects.
READ_BATCH = 50_000

_INTEGERS = {"smallint", "integer", "bigint"}
_FLOATS = {"real", "double precision"}
_TEXT = re.compile(r"^(text|character varying(\(\d+\))?|character(\(\d+\))?)$")
_DECIMAL = re.compile(r"^numeric\((\d+),(\d+)\)$")
# Polars' decimals hold at most this many digits; a wider numeric is read as a float.
_MAX_DECIMAL_DIGITS = 38


def read_as(name: str, stored_type: str) -> tuple[sql.Composable, pl.DataType]:
    """How one column is selected and the type it becomes in the frame.

    An infinite date or timestamp, which Python cannot hold, is read as no value. A numeric too
    wide for Polars' decimals is read as text and then as a float. Any type the profile does not
    measure (jsonb, uuid, arrays) is read as its text.
    """
    column = sql.Identifier(name)
    if stored_type in _INTEGERS:
        return column, pl.Int64()
    if stored_type in _FLOATS:
        return column, pl.Float64()
    if stored_type == "boolean":
        return column, pl.Boolean()
    if stored_type == "date" or stored_type.startswith("timestamp"):
        finite = sql.SQL("CASE WHEN isfinite({c}) THEN {c} END").format(c=column)
        if stored_type == "date":
            return finite, pl.Date()
        zone = "UTC" if stored_type == "timestamp with time zone" else None
        return finite, pl.Datetime("us", zone)
    decimal = _DECIMAL.match(stored_type)
    if decimal and int(decimal[1]) <= _MAX_DECIMAL_DIGITS:
        return column, pl.Decimal(int(decimal[1]), int(decimal[2]))
    if _TEXT.match(stored_type):
        return column, pl.String()
    return sql.SQL("{}::text").format(column), pl.String()


@dataclass(frozen=True)
class StageRows:
    """What was read of a stage table: the rows profiled, their stored types, the table's size."""

    frame: pl.DataFrame
    types: list[tuple[str, str]]
    table_rows: int
    sampled: bool


def stage_columns(conn: psycopg.Connection[Any], schema: str, table: str) -> list[tuple[str, str]]:
    """A table's columns and their stored types, in table order; empty when there is no table."""
    rows = (
        conn.cursor(row_factory=tuple_row)
        .execute(
            "SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute "
            "WHERE attrelid = to_regclass(%s) AND attnum > 0 AND NOT attisdropped "
            "ORDER BY attnum",
            [f"{schema}.{table}"],
        )
        .fetchall()
    )
    return [(name, kind) for name, kind in rows]


def read_stage(
    conn: psycopg.Connection[Any],
    stage: Stage,
    source: str,
    dataset: str,
    row_limit: int = PROFILE_ROW_LIMIT,
) -> StageRows:
    """The stage table as a frame: every row, or a random sample of row_limit rows."""
    schema, table = stage_table(stage, source, dataset)
    with conn.transaction():
        types = stage_columns(conn, schema, table)
        if not types:
            raise LoadError(f"{source}.{dataset} has no {Stage(stage).value} table")
        source_rows, table_rows, _ = sample_rows(conn, sql.Identifier(schema, table), row_limit)
        reads = [read_as(name, kind) for name, kind in types]
        frame_schema = pl.Schema(
            {name: dtype for (name, _), (_, dtype) in zip(types, reads, strict=True)}
        )
        query = sql.SQL("SELECT {} FROM {}").format(
            sql.SQL(", ").join(expression for expression, _ in reads), source_rows
        )
        frame = pl.concat(
            [pl.DataFrame(schema=frame_schema), *_batches(conn, query, frame_schema)],
            how="vertical",
        )
    wide = [
        pl.col(name).cast(pl.Float64, strict=False)
        for name, kind in types
        if kind.startswith("numeric") and isinstance(frame_schema[name], pl.String)
    ]
    return StageRows(frame.with_columns(wide), types, table_rows, table_rows > row_limit)


def _batches(
    conn: psycopg.Connection[Any], query: sql.Composed, schema: pl.Schema
) -> Iterator[pl.DataFrame]:
    with conn.cursor(name="udp_profile_read", row_factory=tuple_row) as cursor:
        cursor.itersize = READ_BATCH
        cursor.execute(query)
        while rows := cursor.fetchmany(READ_BATCH):
            yield pl.DataFrame(rows, schema=schema, orient="row")


def profile_stage(
    conn: psycopg.Connection[Any],
    stage: Stage,
    source: str,
    dataset: str,
    settings: ProfileSettings | None = None,
    row_limit: int = PROFILE_ROW_LIMIT,
) -> StageProfile:
    """Profile the dataset's table at this stage. Raises LoadError when there is no such table."""
    read = read_stage(conn, stage, source, dataset, row_limit)
    return profile_frame(
        read.frame, settings, types=read.types, table_rows=read.table_rows, sampled=read.sampled
    )


def _count(value: int) -> str:
    return f"{value:,}"


def describe(profile: StageProfile, title: str) -> str:
    """The profile as text a person reads in a terminal: the totals, then a line per column."""
    rows = (
        f"{_count(profile.table_rows)} rows, every one profiled"
        if not profile.sampled
        else f"{_count(profile.table_rows)} rows, sampled: {_count(profile.profiled_rows)} profiled"
    )
    on = ", ".join(profile.duplicate_columns) or "no columns"
    lines = [
        f"{title}: {rows}",
        f"Duplicates: {_count(profile.duplicates)}, by {profile.duplicates_by} ({on})",
        f"Missing values: {_count(profile.missing_values)}",
        f"Data-quality problems: {_count(profile.quality_problems)} "
        f"({_count(profile.invalid_values)} values that do not fit their type, "
        f"{_count(profile.missing_required)} required values missing)",
        f"Statistical outliers: {_count(profile.outliers)}",
        "",
    ]
    header = ("column", "type", "missing", "distinct", "invalid", "outliers", "range")
    table = [header]
    for column in profile.columns:
        shown = f"{column.min} to {column.max}" if column.min is not None else ""
        outliers = (
            ""
            if column.outlier_method in (None, "none")
            else f"{_count(column.outliers)} ({column.outlier_method})"
        )
        table.append(
            (
                column.name,
                column.type if column.declared is None else f"{column.type} as {column.declared}",
                _count(column.missing),
                "" if column.distinct is None else _count(column.distinct),
                _count(column.invalid) if column.declared is not None else "",
                outliers,
                shown,
            )
        )
    widths = [max(len(row[index]) for row in table) for index in range(len(header))]
    lines.extend(
        "  ".join(cell.ljust(width) for cell, width in zip(row, widths, strict=True)).rstrip()
        for row in table
    )
    return "\n".join(lines)
