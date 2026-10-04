"""Standardization: a column's type, the column names, and how text values are written."""

from typing import Literal

import polars as pl
from pydantic import Field, model_validator

from udp.config.columns import DeclaredType, storage_dtype
from udp.errors import ValidationError
from udp.pipeline.column_types import check_declared_columns, convert, unfit_values
from udp.pipeline.transform import clean_column_names
from udp.transformations.base import (
    Applied,
    StepConfigError,
    StepContext,
    StepFailed,
    Transformation,
    changed_cells,
    require_columns,
)
from udp.transformations.registry import register


@register
class ConvertType(Transformation):
    """Convert a column to a declared type, by the same rules a load uses.

    A value that does not fit the new type fails the step and nothing changes. Values changed is
    every value converted to a new type, or the values that differ when the type stays the same.
    """

    type = "convert_type"
    column: str
    to: DeclaredType

    def check(self, schema: pl.Schema) -> pl.Schema:
        require_columns("column", [self.column], schema)
        held = schema[self.column]
        try:
            check_declared_columns(pl.Schema({self.column: held}), {self.column: self.to})
        except ValidationError:
            raise StepConfigError(
                "to", f"column '{self.column}' holds {held} and cannot become {self.to}"
            ) from None
        target = storage_dtype(self.to)
        return pl.Schema(
            {name: target if name == self.column else dtype for name, dtype in schema.items()}
        )

    def apply(self, frame: pl.DataFrame, context: StepContext) -> Applied:
        before = frame[self.column]
        after = convert(before, self.to)
        unfit = before.filter(unfit_values(before, after))
        if not unfit.is_empty():
            count = "1 value" if unfit.len() == 1 else f"{unfit.len()} values"
            raise StepFailed(
                f"{count} in column '{self.column}' do not fit {self.to}, the first {unfit[0]!r}"
            )
        changed = changed_cells(before, after) if after.dtype == before.dtype else before.count()
        return Applied(frame.with_columns(after), values_changed=changed)


@register
class NormalizeColumnNames(Transformation):
    """Every column name in lowercase snake_case, unique, as a load cleans them. No value
    changes; the message says how many names did."""

    type = "normalize_column_names"

    def check(self, schema: pl.Schema) -> pl.Schema:
        return pl.Schema(zip(clean_column_names(schema.names()), schema.dtypes(), strict=True))

    def apply(self, frame: pl.DataFrame, context: StepContext) -> Applied:
        cleaned = clean_column_names(frame.columns)
        renamed = sum(old != new for old, new in zip(frame.columns, cleaned, strict=True))
        frame = frame.rename(dict(zip(frame.columns, cleaned, strict=True)))
        return Applied(frame, values_changed=0, message=f"{renamed} column names changed")


@register
class NormalizeValues(Transformation):
    """Trim whitespace, then change the case, then replace whole values by `mapping`, in text
    columns. Values changed is the number of cells that end up different."""

    type = "normalize_values"
    columns: list[str] = Field(min_length=1)
    trim: bool = False
    case: Literal["lower", "upper", "title"] | None = None
    mapping: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _does_something(self) -> NormalizeValues:
        if not self.trim and self.case is None and not self.mapping:
            raise ValueError("needs trim, case, mapping or a mix")
        return self

    def check(self, schema: pl.Schema) -> pl.Schema:
        require_columns("columns", self.columns, schema)
        for name in self.columns:
            if schema[name] != pl.String:
                raise StepConfigError(
                    "columns", f"column '{name}' holds {schema[name]}, and only text is normalised"
                )
        return schema

    def _normalised(self, name: str) -> pl.Expr:
        value = pl.col(name)
        if self.trim:
            value = value.str.strip_chars()
        if self.case == "lower":
            value = value.str.to_lowercase()
        elif self.case == "upper":
            value = value.str.to_uppercase()
        elif self.case == "title":
            value = value.str.to_titlecase()
        if self.mapping:
            value = value.replace(self.mapping)
        return value

    def apply(self, frame: pl.DataFrame, context: StepContext) -> Applied:
        result = frame.with_columns(self._normalised(name) for name in self.columns)
        changed = sum(changed_cells(frame[name], result[name]) for name in self.columns)
        return Applied(result, values_changed=changed)
