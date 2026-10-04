"""Outliers: values below a lower or above an upper percentile of a number column.

The bounds are taken over the whole frame with nearest interpolation, so each is a value that is
in the data and a capped column keeps its type. Nulls and NaNs are never outliers. `flag` adds a
`<column>_outlier` column, `cap` moves each outlier to the bound it passed, `remove` drops its
row; values changed is the number of outliers flagged or capped.
"""

from typing import Literal

import polars as pl
from pydantic import Field, model_validator

from udp.transformations.base import (
    Applied,
    StepConfigError,
    StepContext,
    Transformation,
    require_columns,
)
from udp.transformations.registry import register


@register
class Outliers(Transformation):
    type = "outliers"
    column: str
    action: Literal["flag", "cap", "remove"]
    lower_percentile: float = Field(default=1, ge=0, le=100)
    upper_percentile: float = Field(default=99, ge=0, le=100)

    @model_validator(mode="after")
    def _lower_below_upper(self) -> Outliers:
        if self.lower_percentile >= self.upper_percentile:
            raise ValueError("lower_percentile must be below upper_percentile")
        return self

    @property
    def flag_column(self) -> str:
        return f"{self.column}_outlier"

    def check(self, schema: pl.Schema) -> pl.Schema:
        require_columns("column", [self.column], schema)
        dtype = schema[self.column]
        if not dtype.is_numeric():
            raise StepConfigError(
                "column", f"column '{self.column}' holds {dtype}, and outliers need a number column"
            )
        if self.action == "flag":
            if self.flag_column in schema:
                raise StepConfigError("action", f"column '{self.flag_column}' is already there")
            return pl.Schema({**schema, self.flag_column: pl.Boolean()})
        return schema

    def apply(self, frame: pl.DataFrame, context: StepContext) -> Applied:
        series = frame[self.column]
        if series.dtype.is_float():
            series = series.fill_nan(None)
        lower = series.quantile(self.lower_percentile / 100, interpolation="nearest")
        upper = series.quantile(self.upper_percentile / 100, interpolation="nearest")
        if lower is None or upper is None:
            outside = pl.repeat(False, frame.height, eager=True)
            bounds = "the column has no values"
        else:
            outside = ((series < lower) | (series > upper)).fill_null(False)
            bounds = f"outside {lower} to {upper}"
        found = int(outside.sum())
        message = f"{found} outliers {bounds}"
        if self.action == "remove":
            return Applied(frame.filter(~outside), values_changed=0, message=message)
        if self.action == "flag":
            flagged = frame.with_columns(outside.alias(self.flag_column))
            return Applied(flagged, values_changed=found, message=message)
        if lower is None or upper is None:
            return Applied(frame, values_changed=0, message=message)
        capped = frame.with_columns(
            pl.col(self.column).clip(
                pl.lit(lower).cast(series.dtype), pl.lit(upper).cast(series.dtype)
            )
        )
        return Applied(capped, values_changed=found, message=message)
