"""The profiling engine: a profile of any table, given as a Polars frame.

It works on a frame so that it is the same code whichever stage a table is at and wherever it
is called from — a pipeline step, the CLI or the API — and so that every quantity it reports
can be tested without a database. `udp.profiling.stage` reads a stage table into a frame.

What it reports, per column and for the whole table:

- **descriptive**: the dashboard's view (missing values, range, histogram, values, pattern),
  computed by the same rules as the SQL profile in `udp.profiling.table`, plus the number of
  distinct values of every column;
- **data-quality problems**: a value that does not fit the column's declared type (`convert`
  then `unfit_values`, the rule a load quarantines rows by), including an invalid date, a missing
  value in a column that must have one (the primary key's, or one with a `not_null` check);
- **statistical outliers**: among the numbers that are valid and finite only, the values outside
  the column's `OutlierRule` bounds (IQR by default);
- **duplicates**: rows that repeat an earlier row on the dataset's primary key when it declares
  one and the table has its columns (a row missing part of its key is not counted), and on all
  of the table's columns otherwise.
"""

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal, localcontext

import polars as pl
from pydantic import ValidationError as PydanticValidationError

from udp.config.quality import NotNull
from udp.connectors.base import DatasetBase
from udp.errors import ValidationError
from udp.names import RESERVED_COLUMNS
from udp.pipeline.column_types import check_declared_columns, convert, unfit_values
from udp.profiling.models import (
    OutlierRule,
    ProfileKind,
    StageColumnProfile,
    StageProfile,
    ValueCount,
    json_value,
)
from udp.profiling.table import (
    HISTOGRAM_BARS,
    LIST_ALL_BELOW,
    MAX_SHAPES,
    SHOWN_LENGTH,
    SHOWN_VALUES,
    best_pattern,
    kind_of,
)
from udp.storage.loader import table_columns


@dataclass(frozen=True)
class ProfileSettings:
    """What a profile needs to know beyond the table itself.

    `declared` maps a column to its declared type, `key` is the primary key duplicates are
    counted on, `required` the columns that must have a value, and `outliers` the outlier rule
    of a column whose rule is not `default_outliers`.
    """

    declared: Mapping[str, str] = field(default_factory=dict)
    key: Sequence[str] = ()
    required: Sequence[str] = ()
    outliers: Mapping[str, OutlierRule] = field(default_factory=dict)
    default_outliers: OutlierRule = field(default_factory=OutlierRule)

    @classmethod
    def for_dataset(
        cls,
        dataset: DatasetBase,
        outliers: Mapping[str, OutlierRule] | None = None,
        default_outliers: OutlierRule | None = None,
    ) -> ProfileSettings:
        """The settings a dataset's source.yaml declares: its columns, key and not_null checks."""
        key = tuple(dataset.primary_key or ())
        not_null = [check.column for check in dataset.checks if isinstance(check, NotNull)]
        return cls(
            declared=dict(dataset.columns),
            key=key,
            required=tuple(dict.fromkeys([*key, *not_null])),
            outliers=dict(outliers or {}),
            default_outliers=default_outliers or OutlierRule(),
        )


def parse_outlier_rule(text: str) -> tuple[str | None, OutlierRule]:
    """`[COLUMN=]METHOD[:SETTINGS]` as (the column, or None for every column, and the rule).

    METHOD is `iqr` with an optional factor (`iqr:3`), `percentile` with optional lower and
    upper percentiles (`percentile:5:95`), or `none`. Raises ValueError for anything else.
    """
    named, equals, rule = text.rpartition("=")
    column = named.strip() or None
    if equals and column is None:
        raise ValueError(f"'{text}' names no column before '='")
    method, *numbers = rule.strip().split(":")
    try:
        values = [float(number) for number in numbers]
    except ValueError:
        raise ValueError(f"'{text}': the settings after the method are numbers") from None
    settings: dict[str, float]
    if method == "iqr" and len(values) <= 1:
        settings = {"k": values[0]} if values else {}
    elif method == "percentile" and len(values) in (0, 2):
        settings = {"lower": values[0], "upper": values[1]} if values else {}
    elif method == "none" and not values:
        settings = {}
    else:
        raise ValueError(f"'{text}' is not iqr, iqr:K, percentile, percentile:LOWER:UPPER or none")
    try:
        return column, OutlierRule.model_validate({"method": method, **settings})
    except PydanticValidationError as error:
        problem = error.errors()[0]
        where = ".".join(str(part) for part in problem["loc"])
        raise ValueError(f"'{text}': {where + ' ' if where else ''}{problem['msg']}") from None


def outlier_bounds(values: pl.Series, rule: OutlierRule) -> tuple[float, float] | None:
    """The range outside which a value is an outlier, from finite numbers with no nulls; None
    when the rule looks for none or there is nothing to look at."""
    if rule.method == "none" or values.is_empty():
        return None
    if rule.method == "iqr":
        q1 = _quantile(values, 0.25)
        q3 = _quantile(values, 0.75)
        spread = q3 - q1
        return q1 - rule.k * spread, q3 + rule.k * spread
    return _quantile(values, rule.lower / 100), _quantile(values, rule.upper / 100)


def _quantile(values: pl.Series, share: float) -> float:
    found = values.quantile(share, interpolation="linear")
    assert found is not None  # values is never empty here
    return float(found)


def profile_frame(
    frame: pl.DataFrame,
    settings: ProfileSettings | None = None,
    *,
    types: Sequence[tuple[str, str]] | None = None,
    table_rows: int | None = None,
    sampled: bool = False,
) -> StageProfile:
    """Profile a table given as a frame.

    `types` are the columns' stored types as PostgreSQL's format_type spells them, which decide
    what is shown for each; by default, the types the loader would store the frame's columns as.
    When the frame is a sample, `table_rows` is the whole table's row count and `sampled` is true.
    Platform columns (`_run_id` and the like) are left out.
    """
    settings = settings or ProfileSettings()
    stored = dict(types if types is not None else table_columns(frame))
    names = [name for name in frame.columns if name not in RESERVED_COLUMNS]
    required = set(settings.required)
    columns = [
        _column(
            frame[name],
            stored[name],
            settings.declared.get(name),
            name in required,
            settings.outliers.get(name, settings.default_outliers),
        )
        for name in names
    ]
    key = list(settings.key)
    by_key = bool(key) and all(name in frame.columns for name in key)
    counted_on = key if by_key else names
    # A row with no key is a missing required value already, not a copy of another keyless row.
    compared = frame.select(counted_on).drop_nulls() if by_key else frame.select(counted_on)
    duplicates = compared.height - compared.n_unique() if counted_on else 0
    invalid = sum(column.invalid for column in columns)
    missing_required = sum(column.missing_required for column in columns)
    return StageProfile(
        table_rows=frame.height if table_rows is None else table_rows,
        profiled_rows=frame.height,
        sampled=sampled,
        columns=columns,
        duplicates=duplicates,
        duplicates_by="key" if by_key else "all columns",
        duplicate_columns=counted_on,
        missing_values=sum(column.missing for column in columns),
        invalid_values=invalid,
        missing_required=missing_required,
        quality_problems=invalid + missing_required,
        outliers=sum(column.outliers for column in columns),
    )


def _column(
    series: pl.Series,
    stored_type: str,
    declared: str | None,
    required: bool,
    rule: OutlierRule,
) -> StageColumnProfile:
    kind = kind_of(stored_type)
    present = series.drop_nulls()
    profile = StageColumnProfile(
        name=series.name,
        type=stored_type,
        kind=kind,
        missing=series.null_count(),
        distinct=present.n_unique(),
        declared=declared,
        required=required,
    )
    if kind in ("number", "date"):
        _add_range(present, kind, profile)
    elif kind == "text":
        _add_values(present, stored_type, profile)
    valid = series
    if declared is not None:
        valid = _declared_values(series, declared)
        profile.invalid = int(unfit_values(series, valid).sum())
    if required:
        profile.missing_required = profile.missing
    if valid.dtype.is_numeric():
        _add_outliers(valid, rule, profile)
    return profile


def _declared_values(series: pl.Series, declared: str) -> pl.Series:
    """The column in its declared type, null where a value does not fit — every value, when the
    column holds a type that can never become the declared one."""
    try:
        check_declared_columns(pl.Schema({series.name: series.dtype}), {series.name: declared})
    except ValidationError:
        return pl.Series(series.name, [None] * series.len(), dtype=pl.Null)
    return convert(series, declared)


def _finite_numbers(values: pl.Series) -> pl.Series:
    numbers = values.drop_nulls().to_physical().cast(pl.Float64)
    return numbers.filter(numbers.is_finite())


def _add_range(present: pl.Series, kind: ProfileKind, profile: StageColumnProfile) -> None:
    finite = present.filter(present.is_finite()) if present.dtype.is_float() else present
    if finite.is_empty():
        profile.histogram = [0] * HISTOGRAM_BARS
        return
    profile.min = json_value(finite.min())
    profile.max = json_value(finite.max())
    if kind == "number":
        mean = finite.cast(pl.Float64).mean()
        if isinstance(mean, float) and math.isfinite(mean):
            with localcontext() as context:
                # Room for the largest float's 309 digits and the four places shown.
                context.prec = 400
                rounded = Decimal(repr(mean)).quantize(Decimal("0.0001"), ROUND_HALF_UP)
            profile.mean = str(rounded)
    profile.histogram = histogram(_finite_numbers(finite))


def histogram(numbers: pl.Series) -> list[int]:
    """HISTOGRAM_BARS equal-width bars from the lowest number to the highest, the highest in
    the last bar — PostgreSQL's width_bucket, as the SQL profile draws them."""
    bars = [0] * HISTOGRAM_BARS
    if numbers.is_empty():
        return bars
    low, high = _quantile(numbers, 0.0), _quantile(numbers, 1.0)
    if low == high:
        bars[0] = numbers.len()
        return bars
    # Polars divides by multiplying by one over the divisor, so the range and one over it must
    # both be finite floats: a range wider than the largest float is halved, and one so narrow
    # that one over it overflows is scaled up. A power of two moves no number between bars.
    scale = 1.0
    if math.isinf(high - low):
        scale = 0.5
    elif math.isinf(1 / (high - low)):
        scale = 2.0**600
    found = (
        numbers.to_frame("x")
        .select(
            (
                (
                    (pl.col("x") * scale - low * scale)
                    / (high * scale - low * scale)
                    * HISTOGRAM_BARS
                ).floor()
                + 1
            )
            .clip(1, HISTOGRAM_BARS)
            .cast(pl.Int64)
            .alias("bar")
        )
        .group_by("bar")
        .len()
    )
    for bar, count in found.iter_rows():
        bars[bar - 1] += count
    return bars


def _add_values(present: pl.Series, stored_type: str, profile: StageColumnProfile) -> None:
    values = present.cast(pl.String).alias("v")
    counts = values.value_counts(name="n")
    profile.appear_once = int((counts["n"] == 1).sum())
    top = counts.sort(["n", "v"], descending=[True, False]).head(LIST_ALL_BELOW - 1)
    shown = [ValueCount(value=v[:SHOWN_LENGTH], count=n) for v, n in top.iter_rows()]
    if counts.height < LIST_ALL_BELOW:
        profile.all_values = shown
    else:
        profile.most_used = shown[:SHOWN_VALUES]
        if counts["n"].min() != counts["n"].max():
            first = {value.value for value in profile.most_used}
            bottom = counts.sort(["n", "v"]).head(SHOWN_VALUES * 2)
            least = [
                ValueCount(value=v[:SHOWN_LENGTH], count=n)
                for v, n in bottom.iter_rows()
                if v[:SHOWN_LENGTH] not in first
            ]
            profile.least_used = least[:SHOWN_VALUES]
    if stored_type == "boolean":
        # "true" and "false" share a shape, so every boolean column would show ^[a-z]+$.
        return
    shapes = (
        values.str.replace_all("[0-9]", "9")
        .str.replace_all("[A-Z]", "A")
        .str.replace_all("[a-z]", "a")
        .value_counts(name="n")
    )
    if shapes.height > MAX_SHAPES:
        return
    pattern = best_pattern([(shape, int(n)) for shape, n in shapes.iter_rows()], values.len())
    if pattern is not None:
        profile.pattern, profile.pattern_share = pattern


def _add_outliers(valid: pl.Series, rule: OutlierRule, profile: StageColumnProfile) -> None:
    profile.outlier_method = rule.method
    numbers = _finite_numbers(valid)
    bounds = outlier_bounds(numbers, rule)
    if bounds is None:
        return
    profile.outlier_low, profile.outlier_high = bounds
    profile.outliers = int(((numbers < bounds[0]) | (numbers > bounds[1])).sum())
