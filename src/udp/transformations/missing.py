"""Missing values: drop the rows that have them, or fill them with a mean, median, mode or value.

A mean, median or mode is taken over the whole frame. On an integer column the mean or median is
rounded to the nearest whole number, so the column keeps its type. A column with no values has
no mean, median or mode: the step fails and the nulls stay — it never writes a zero.
"""

from typing import Literal

import polars as pl
from pydantic import Field, model_validator

from udp.transformations.base import (
    Applied,
    StepConfigError,
    StepContext,
    StepFailed,
    Transformation,
    require_columns,
)
from udp.transformations.registry import register


@register
class DropMissing(Transformation):
    """Drop every row with a null in one of `columns`, or in any column when none are named."""

    type = "drop_missing"
    columns: list[str] | None = Field(default=None, min_length=1)

    def check(self, schema: pl.Schema) -> pl.Schema:
        require_columns("columns", self.columns or [], schema)
        return schema

    def apply(self, frame: pl.DataFrame, context: StepContext) -> Applied:
        return Applied(frame.drop_nulls(self.columns), values_changed=0)


@register
class FillMissing(Transformation):
    """Fill the nulls in `columns`; values changed is the number of nulls filled."""

    type = "fill_missing"
    columns: list[str] = Field(min_length=1)
    method: Literal["mean", "median", "mode", "value"]
    value: bool | int | float | str | None = None

    @model_validator(mode="after")
    def _value_only_for_value(self) -> FillMissing:
        if self.method == "value" and self.value is None:
            raise ValueError("method 'value' needs a value")
        if self.method != "value" and self.value is not None:
            raise ValueError(f"a value is only used with method 'value', not '{self.method}'")
        return self

    def check(self, schema: pl.Schema) -> pl.Schema:
        require_columns("columns", self.columns, schema)
        for name in self.columns:
            dtype = schema[name]
            if self.method in ("mean", "median") and not dtype.is_numeric():
                raise StepConfigError(
                    "columns",
                    f"column '{name}' holds {dtype}, and a {self.method} needs a number column",
                )
            if self.method == "value":
                try:
                    pl.Series([self.value]).cast(dtype, strict=True)
                except pl.exceptions.PolarsError:
                    raise StepConfigError(
                        "value", f"{self.value!r} does not fit column '{name}', which holds {dtype}"
                    ) from None
        return schema

    def _fill(self, series: pl.Series) -> pl.Series:
        if self.method == "value":
            return pl.Series([self.value]).cast(series.dtype)
        present = series.drop_nulls()
        if present.is_empty():
            raise StepFailed(
                f"column '{series.name}' has no values to take a {self.method} of; "
                "its nulls are left as they are"
            )
        if self.method == "mode":
            return present.mode().sort().head(1)
        found = present.mean() if self.method == "mean" else present.median()
        if series.dtype.is_integer():
            found = round(float(found))  # type: ignore[arg-type]
        return pl.Series([found]).cast(series.dtype)

    def apply(self, frame: pl.DataFrame, context: StepContext) -> Applied:
        fills = [self._fill(frame[name]) for name in self.columns]
        filled = frame.with_columns(
            pl.col(name).fill_null(pl.lit(fill).first())
            for name, fill in zip(self.columns, fills, strict=True)
        )
        changed = sum(frame[name].null_count() for name in self.columns)
        return Applied(filled, values_changed=changed)
