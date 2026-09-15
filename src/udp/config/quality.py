"""Quality checks a dataset declares under `checks:` in source.yaml.

Row checks (not_null, accepted_values, range, regex) look at every row before the load; an
error-level failure sends the row to quarantine. Table checks (unique, min_rows, freshness,
row_count_change) look at the loaded table; an error-level failure fails the run.
"""

from datetime import date, datetime, timedelta
from typing import Annotated, Literal

import polars as pl
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class CheckBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    severity: Literal["warn", "error"] = "error"

    @property
    def columns_checked(self) -> list[str]:
        column = getattr(self, "column", None)
        return [column] if column is not None else list(getattr(self, "columns", []))


class NotNull(CheckBase):
    check: Literal["not_null"]
    column: str


class AcceptedValues(CheckBase):
    check: Literal["accepted_values"]
    column: str
    values: list[bool | int | float | str] = Field(min_length=1)


class Range(CheckBase):
    check: Literal["range"]
    column: str
    min: int | float | datetime | date | None = None
    max: int | float | datetime | date | None = None

    @model_validator(mode="after")
    def _has_a_bound(self) -> Range:
        if self.min is None and self.max is None:
            raise ValueError("needs min, max or both")
        if self.min is not None and self.max is not None:
            try:
                wrong_way = self.min > self.max  # type: ignore[operator]
            except TypeError:
                raise ValueError("min and max must be the same kind of value") from None
            if wrong_way:
                raise ValueError("min must not be larger than max")
        return self


class Regex(CheckBase):
    check: Literal["regex"]
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


class Unique(CheckBase):
    check: Literal["unique"]
    columns: list[str] = Field(min_length=1)


class MinRows(CheckBase):
    check: Literal["min_rows"]
    rows: int = Field(ge=0)


class Freshness(CheckBase):
    check: Literal["freshness"]
    column: str
    max_age: timedelta

    @field_validator("max_age")
    @classmethod
    def _positive(cls, value: timedelta) -> timedelta:
        if value <= timedelta(0):
            raise ValueError("must be longer than zero")
        return value


class RowCountChange(CheckBase):
    check: Literal["row_count_change"]
    max_percent: float = Field(gt=0)


Check = Annotated[
    NotNull | AcceptedValues | Range | Regex | Unique | MinRows | Freshness | RowCountChange,
    Field(discriminator="check"),
]
ROW_CHECKS = (NotNull, AcceptedValues, Range, Regex)


def full_match(pattern: str) -> str:
    """A regex that matches only when the whole value matches `pattern`."""
    return rf"\A(?:{pattern})\z"
