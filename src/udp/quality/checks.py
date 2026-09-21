"""Running a dataset's quality checks.

Row checks run on every chunk before the load: each row failing an error-level row check is
quarantined, and every row check records how many rows failed it. Table checks run on the
loaded table inside the run's transaction, and a failed error-level table check fails the run.
"""

from collections.abc import Iterator, Sequence
from datetime import UTC, date, datetime, time
from decimal import Decimal
from fractions import Fraction
from typing import Any

import polars as pl
import structlog

from udp.config.quality import (
    ROW_CHECKS,
    AcceptedValues,
    Check,
    Freshness,
    MinRows,
    NotNull,
    Range,
    Regex,
    RowCountChange,
    Unique,
    full_match,
)
from udp.connectors.base import DatasetBase
from udp.errors import QualityError, ValidationError
from udp.quality.quarantine import quarantine
from udp.storage.loader import CheckResult, LoadTransaction, RunFindings

log = structlog.get_logger(step="quality")

RowCheck = NotNull | AcceptedValues | Range | Regex


def _settings(check: Check) -> dict[str, Any]:
    return check.model_dump(mode="json", exclude={"check", "severity"})


def _is_number(dtype: pl.DataType) -> bool:
    return dtype.is_integer() or dtype.is_float() or isinstance(dtype, pl.Decimal)


def _literal(value: Any, dtype: pl.DataType) -> pl.Expr:
    """A check's value as a literal comparable with the column, exactly."""
    if isinstance(dtype, pl.Decimal) and isinstance(value, float | int):
        return pl.lit(Decimal(repr(value)))
    if isinstance(dtype, pl.Datetime):
        moment = value if isinstance(value, datetime) else datetime.combine(value, time())
        if dtype.time_zone and moment.tzinfo is None:
            moment = moment.replace(tzinfo=UTC)
        elif not dtype.time_zone and moment.tzinfo is not None:
            moment = moment.astimezone(UTC).replace(tzinfo=None)
        return pl.lit(moment, dtype=dtype)
    return pl.lit(value)


def _declare_hint(dtype: pl.DataType) -> str:
    """For a text column, how to make it comparable: numbers and dates often arrive as text."""
    if isinstance(dtype, pl.String):
        return "; declare its type under columns: (for example decimal(10,2) or date)"
    return ""


def check_columns(schema: pl.Schema, checks: Sequence[Check]) -> None:
    """Every check names columns that exist, of a type the check can compare."""
    for position, check in enumerate(checks):
        where = f"check {position} ({check.check})"
        for name in check.columns_checked:
            if name not in schema:
                raise ValidationError(f"{where}: column '{name}' is not in the data")
        column: str | None = getattr(check, "column", None)
        dtype = schema[column] if column is not None else None
        if isinstance(check, Regex) and not isinstance(dtype, pl.String | pl.Null):
            raise ValidationError(f"{where}: column '{column}' is {dtype}, not text")
        if isinstance(check, Range) and dtype is not None:
            bounds = [bound for bound in (check.min, check.max) if bound is not None]
            numbers = all(isinstance(b, int | float) and not isinstance(b, bool) for b in bounds)
            if _is_number(dtype):
                fits = numbers
            elif isinstance(dtype, pl.Date):
                fits = all(isinstance(b, date) and not isinstance(b, datetime) for b in bounds)
            elif isinstance(dtype, pl.Datetime):
                fits = all(isinstance(b, date) for b in bounds)
            else:
                fits = isinstance(dtype, pl.Null)
            if not fits:
                raise ValidationError(
                    f"{where}: column '{column}' is {dtype} and cannot be compared with "
                    f"{', '.join(repr(b) for b in bounds)}{_declare_hint(dtype)}"
                )
        if isinstance(check, Freshness) and not isinstance(dtype, pl.Date | pl.Datetime | pl.Null):
            raise ValidationError(f"{where}: column '{column}' is {dtype}, not a date or timestamp")
        if isinstance(check, AcceptedValues) and dtype is not None:
            if isinstance(dtype, pl.String):
                fits = all(isinstance(v, str) for v in check.values)
            elif isinstance(dtype, pl.Boolean):
                fits = all(isinstance(v, bool) for v in check.values)
            elif _is_number(dtype):
                fits = all(
                    isinstance(v, int | float) and not isinstance(v, bool) for v in check.values
                )
            else:
                fits = isinstance(dtype, pl.Null)
            if not fits:
                raise ValidationError(
                    f"{where}: column '{column}' is {dtype} and cannot be compared with its values"
                    f"{_declare_hint(dtype)}"
                )


def _failing(check: RowCheck, dtype: pl.DataType) -> pl.Expr:
    """True for rows that fail the check; null values pass every row check but not_null."""
    value = pl.col(check.column)
    if isinstance(check, NotNull):
        return value.is_null()
    if isinstance(dtype, pl.Null):
        return pl.lit(False)
    if isinstance(check, AcceptedValues):
        accepted = pl.any_horizontal([value == _literal(v, dtype) for v in check.values])
        return value.is_not_null() & ~accepted
    if isinstance(check, Range):
        outside = pl.lit(False)
        if check.min is not None:
            outside = outside | (value < _literal(check.min, dtype))
        if check.max is not None:
            outside = outside | (value > _literal(check.max, dtype))
        if dtype.is_float():
            outside = outside | value.is_nan()
        return value.is_not_null() & outside
    return value.is_not_null() & ~value.str.contains(full_match(check.pattern))


def _reason(position: int, check: RowCheck) -> str:
    return f"check {position} {check.check} failed on column '{check.column}'"


def check_rows(
    chunks: Iterator[pl.DataFrame], checks: Sequence[Check], findings: RunFindings
) -> Iterator[pl.DataFrame]:
    """Count rows failing each row check and quarantine those failing an error-level one."""
    row_checks = [
        (position, check) for position, check in enumerate(checks) if isinstance(check, ROW_CHECKS)
    ]
    failing = [0] * len(row_checks)
    checked = False
    for chunk in chunks:
        if not checked:
            check_columns(chunk.schema, checks)
            checked = True
        if not row_checks:
            yield chunk
            continue
        flags = chunk.select(
            _failing(check, chunk.schema[check.column]).fill_null(False).alias(str(index))
            for index, (_, check) in enumerate(row_checks)
        )
        for index in range(len(row_checks)):
            failing[index] += int(flags[str(index)].sum())
        errors = [
            str(index) for index, (_, check) in enumerate(row_checks) if check.severity == "error"
        ]
        if errors:
            rejected = flags.select(pl.any_horizontal(errors)).to_series()
            if rejected.any():
                reasons = (
                    flags.filter(rejected)
                    .select(
                        pl.concat_str(
                            [
                                pl.when(pl.col(str(index))).then(pl.lit(_reason(position, check)))
                                for index, (position, check) in enumerate(row_checks)
                                if check.severity == "error"
                            ],
                            separator="; ",
                            ignore_nulls=True,
                        )
                    )
                    .to_series()
                )
                quarantine(findings, chunk.filter(rejected), reasons)
                chunk = chunk.filter(~rejected)
        yield chunk
    for index, (position, check) in enumerate(row_checks):
        count = failing[index]
        findings.results.append(
            CheckResult(
                position=position,
                check_type=check.check,
                columns=(check.column,),
                severity=check.severity,
                passed=count == 0,
                failing_rows=count,
                table_rows=None,
                message=f"{count} rows failed" if count else "every row passed",
                settings=_settings(check),
            )
        )


def _as_utc(value: date | datetime) -> datetime:
    if not isinstance(value, datetime):
        return datetime.combine(value, time(), UTC)
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _table_check(
    transaction: LoadTransaction,
    table: str,
    findings: RunFindings,
    position: int,
    check: Unique | MinRows | Freshness | RowCountChange,
    started_at: datetime,
) -> CheckResult:
    failing_rows: int | None = None
    table_rows: int | None = None
    if isinstance(check, Unique):
        failing_rows = transaction.duplicate_rows(table, check.columns)
        passed = failing_rows == 0
        message = f"{failing_rows} rows share their {', '.join(check.columns)} with another row"
    elif isinstance(check, MinRows):
        table_rows = transaction.table_rows(table)
        passed = table_rows >= check.rows
        message = f"the table has {table_rows} rows, at least {check.rows} required"
    elif isinstance(check, Freshness):
        newest = transaction.newest_value(table, check.column)
        if newest is None:
            passed = False
            message = f"column '{check.column}' has no values"
        else:
            age = started_at - _as_utc(newest)
            passed = age <= check.max_age
            message = f"newest '{check.column}' is {age} old, at most {check.max_age} allowed"
    else:
        table_rows = transaction.table_rows(table)
        previous = transaction.previous_table_rows(findings.source, findings.dataset)
        if previous is None:
            passed = True
            message = f"the table has {table_rows} rows; no earlier count to compare"
        elif previous == 0:
            passed = table_rows == 0
            message = f"the table has {table_rows} rows, the last successful run counted 0"
        else:
            change = Fraction(abs(table_rows - previous) * 100, previous)
            passed = change <= Fraction(repr(check.max_percent))
            message = (
                f"the table has {table_rows} rows, {float(change):.2f}% different from "
                f"{previous}, at most {check.max_percent}% allowed"
            )
    return CheckResult(
        position=position,
        check_type=check.check,
        columns=tuple(check.columns_checked),
        severity=check.severity,
        passed=passed,
        failing_rows=failing_rows,
        table_rows=table_rows,
        message=message,
        settings=_settings(check),
    )


def check_dataset(
    transaction: LoadTransaction,
    table: str,
    dataset: DatasetBase,
    findings: RunFindings,
    started_at: datetime,
) -> None:
    """Run the table checks; an error-level failure raises QualityError after all have run."""
    failed = []
    for position, check in enumerate(dataset.checks):
        if isinstance(check, Unique | MinRows | Freshness | RowCountChange):
            result = _table_check(transaction, table, findings, position, check, started_at)
            findings.results.append(result)
            if not result.passed and result.severity == "error":
                failed.append(f"check {position} {check.check}: {result.message}")
    findings.results.sort(key=lambda result: result.position)
    passed = sum(result.passed for result in findings.results)
    log.info(
        "quality checked",
        passed=passed,
        failed=len(findings.results) - passed,
        failed_error_level=sum(
            not result.passed and result.severity == "error" for result in findings.results
        ),
        rows_quarantined=findings.quarantined_rows,
    )
    if failed:
        raise QualityError("; ".join(failed))
