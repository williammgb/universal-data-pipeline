"""What a profile is: the dashboard's column-by-column view, and the stored profile of a stage.

`DatasetProfile` is what the dashboard's profile tab shows and the API returns for it.
`StageProfile` is what the profiling engine stores for a RAW, STAGING or CLEAN table: every field
of the dashboard's view, plus duplicates, data-quality problems and statistical outliers, which
are counted apart and never mixed.
"""

import math
from datetime import date, time
from decimal import Decimal
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

JsonValue = str | int | float | bool | None
ProfileKind = Literal["number", "date", "text", "other"]


def json_value(value: Any) -> JsonValue:
    """A stored value as JSON: exact decimals and non-finite floats become text."""
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, date | time):  # datetime is a date
        return value.isoformat()
    return str(value)  # UUID, and any other stored type, as its text form


class ValueCount(BaseModel):
    value: JsonValue
    count: int


class ColumnProfile(BaseModel):
    name: str
    type: str
    kind: ProfileKind
    missing: int
    # number and date columns: the finite range, and a 20-bar histogram between its ends
    min: JsonValue = None
    max: JsonValue = None
    mean: JsonValue = None
    histogram: list[int] | None = None
    # text columns: every value when there are few, otherwise the most and least used
    distinct: int | None = None
    appear_once: int | None = None
    all_values: list[ValueCount] | None = None
    most_used: list[ValueCount] | None = None
    least_used: list[ValueCount] | None = None
    pattern: str | None = None
    pattern_share: float | None = None


class DatasetProfile(BaseModel):
    table_rows: int
    profiled_rows: int
    sampled: bool
    columns: list[ColumnProfile]


OutlierMethod = Literal["iqr", "percentile", "none"]
IQR_FACTOR = 1.5
LOWER_PERCENTILE = 1.0
UPPER_PERCENTILE = 99.0


class OutlierRule(BaseModel):
    """How one column's statistical outliers are found. The default is IQR with k = 1.5.

    - `iqr`: a value below Q1 - k*IQR or above Q3 + k*IQR, where IQR = Q3 - Q1 (Tukey's fences).
    - `percentile`: a value below the `lower` percentile or above the `upper` one.
    - `none`: the column is not looked at for outliers.

    Quartiles and percentiles are interpolated linearly between the two nearest values.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    method: OutlierMethod = "iqr"
    k: float = Field(default=IQR_FACTOR, gt=0, allow_inf_nan=False)
    lower: float = Field(default=LOWER_PERCENTILE, ge=0, le=100)
    upper: float = Field(default=UPPER_PERCENTILE, ge=0, le=100)

    @model_validator(mode="after")
    def _lower_below_upper(self) -> Self:
        if self.lower >= self.upper:
            raise ValueError(f"lower ({self.lower}) must be below upper ({self.upper})")
        return self


class StageColumnProfile(ColumnProfile):
    """One column of a stage profile: the dashboard's view of it, then its quality and outliers.

    A value is exactly one of: missing, invalid (present but not fitting the declared type) or
    valid. Outliers are found among the valid finite numbers only, so a value can never be both
    a quality problem and an outlier.
    """

    declared: str | None = None
    # data-quality problems
    invalid: int = 0
    required: bool = False
    missing_required: int = 0
    # statistical outliers
    outlier_method: OutlierMethod | None = None
    outlier_low: float | None = None
    outlier_high: float | None = None
    outliers: int = 0


class StageProfile(BaseModel):
    """A profile of one dataset at one stage. Counts are of the profiled rows, which are every
    row unless the table was over the row limit and `sampled` is true."""

    table_rows: int
    profiled_rows: int
    sampled: bool
    columns: list[StageColumnProfile]
    duplicates: int
    duplicates_by: Literal["key", "all columns"]
    duplicate_columns: list[str]
    missing_values: int
    invalid_values: int
    missing_required: int
    quality_problems: int
    outliers: int


class BeforeAfter(BaseModel):
    before: int
    after: int


class ProfileComparison(BaseModel):
    """Two profiles of a dataset side by side: what a transformation changed."""

    rows: BeforeAfter
    missing_values: BeforeAfter
    invalid_values: BeforeAfter
    outliers: BeforeAfter
    duplicates: BeforeAfter


def compare_profiles(before: StageProfile, after: StageProfile) -> ProfileComparison:
    """Rows, missing values, invalid values, outliers and duplicates, before and after."""
    return ProfileComparison(
        rows=BeforeAfter(before=before.table_rows, after=after.table_rows),
        missing_values=BeforeAfter(before=before.missing_values, after=after.missing_values),
        invalid_values=BeforeAfter(before=before.invalid_values, after=after.invalid_values),
        outliers=BeforeAfter(before=before.outliers, after=after.outliers),
        duplicates=BeforeAfter(before=before.duplicates, after=after.duplicates),
    )
