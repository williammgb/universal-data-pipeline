"""Constraints a dataset declares under `constraints:` in source.yaml: what it should look like.

A constraint never changes data; a value that breaks one is recorded as a data-quality problem.
Every constraint but `datatype` and `unique` is a V1 row check underneath (`to_check`), so each
rule is written once: `not_null` is not_null, `min` and `max` are range, `allowed_values` is
accepted_values and `pattern` is regex. `unique` is V1's unique; `datatype` is the rule a load
quarantines a value by when it does not fit its declared type.
"""

from datetime import date, datetime
from typing import Annotated, Literal

import polars as pl
from pydantic import BaseModel, ConfigDict, Field, field_validator

from udp.config.columns import DeclaredType
from udp.config.quality import (
    AcceptedValues,
    Check,
    NotNull,
    Range,
    Regex,
    Unique,
    full_match,
)

Bound = int | float | datetime | date


class ConstraintBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    critical: bool = False

    @property
    def columns_checked(self) -> list[str]:
        column = getattr(self, "column", None)
        return [column] if column is not None else list(getattr(self, "columns", []))


class Datatype(ConstraintBase):
    constraint: Literal["datatype"]
    column: str
    type: DeclaredType


class NotNullConstraint(ConstraintBase):
    constraint: Literal["not_null"]
    column: str


class Min(ConstraintBase):
    constraint: Literal["min"]
    column: str
    value: Bound


class Max(ConstraintBase):
    constraint: Literal["max"]
    column: str
    value: Bound


class UniqueConstraint(ConstraintBase):
    constraint: Literal["unique"]
    columns: list[str] = Field(min_length=1)


class AllowedValues(ConstraintBase):
    constraint: Literal["allowed_values"]
    column: str
    values: list[bool | int | float | str] = Field(min_length=1)


class Pattern(ConstraintBase):
    constraint: Literal["pattern"]
    column: str
    pattern: str

    @field_validator("pattern")
    @classmethod
    def _compiles(cls, value: str) -> str:
        try:
            pl.select(pl.lit("").str.contains(full_match(value)))
        except pl.exceptions.PolarsError as error:
            raise ValueError(f"is not a valid regular expression: {error}") from None
        return value


Constraint = Annotated[
    Datatype | NotNullConstraint | Min | Max | UniqueConstraint | AllowedValues | Pattern,
    Field(discriminator="constraint"),
]


def to_check(constraint: Constraint) -> Check | None:
    """The V1 check that tests the same rule, or None for a datatype constraint, which has none."""
    if isinstance(constraint, NotNullConstraint):
        return NotNull(check="not_null", column=constraint.column)
    if isinstance(constraint, Min):
        return Range(check="range", column=constraint.column, min=constraint.value)
    if isinstance(constraint, Max):
        return Range(check="range", column=constraint.column, max=constraint.value)
    if isinstance(constraint, UniqueConstraint):
        return Unique(check="unique", columns=constraint.columns)
    if isinstance(constraint, AllowedValues):
        return AcceptedValues(
            check="accepted_values", column=constraint.column, values=constraint.values
        )
    if isinstance(constraint, Pattern):
        return Regex(check="regex", column=constraint.column, pattern=constraint.pattern)
    return None
