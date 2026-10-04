"""What every transformation is: a named type, validated settings, and a step that runs on its own.

A transformation is a pydantic model holding its settings. `check` is the load-time test of
those settings against the columns the step will meet; `apply` changes a frame and says how many
values it changed. Steps run over a whole frame — the dataset's materialised stage table — never
chunk by chunk, so a mean or a percentile is taken over every row. They take and return frames
and never write a table, so none of them can touch RAW.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import ClassVar, Literal

import polars as pl
from pydantic import BaseModel, ConfigDict

from udp.config.constraints import Constraint

StepStatus = Literal["succeeded", "failed"]


class StepConfigError(Exception):
    """A step's settings do not fit the data: `field` is the setting that is wrong."""

    def __init__(self, field: str, message: str) -> None:
        super().__init__(message)
        self.field = field
        self.message = message


class StepFailed(Exception):
    """A step could not do what it was asked; the frame is left as it was."""


@dataclass(frozen=True)
class StepContext:
    """What a step may need from the dataset beyond the frame itself."""

    constraints: Sequence[Constraint] = ()
    primary_key: Sequence[str] | None = None


@dataclass(frozen=True)
class Applied:
    frame: pl.DataFrame
    values_changed: int
    message: str | None = None


class StepResult(BaseModel):
    """One step's outcome, as a person reads it."""

    position: int
    type: str
    status: StepStatus
    rows_in: int
    rows_out: int
    values_changed: int
    error: str | None = None
    message: str | None = None


class Transformation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    # The name a step is declared by (`type: fill_missing`); set by every registered class.
    type: ClassVar[str]

    def check(self, schema: pl.Schema) -> pl.Schema:
        """The schema after this step; raises StepConfigError when the settings do not fit."""
        return schema

    def apply(self, frame: pl.DataFrame, context: StepContext) -> Applied:
        raise NotImplementedError


def changed_cells(before: pl.Series, after: pl.Series) -> int:
    """How many cells differ, a null and a value counting as different."""
    return int(before.ne_missing(after).sum())


def require_columns(field: str, names: Sequence[str], schema: pl.Schema) -> None:
    """Each named column is in the data, and named once."""
    seen: set[str] = set()
    for name in names:
        if name not in schema:
            raise StepConfigError(field, f"column '{name}' is not in the data")
        if name in seen:
            raise StepConfigError(field, f"column '{name}' is named more than once")
        seen.add(name)


def run_step(
    frame: pl.DataFrame,
    step: Transformation,
    position: int,
    context: StepContext | None = None,
) -> tuple[pl.DataFrame, StepResult]:
    """Run one step. Never raises: a step that fails returns the frame unchanged with the error."""
    try:
        applied = step.apply(frame, context or StepContext())
    except Exception as error:
        reason = str(error) if isinstance(error, StepFailed) else f"{type(error).__name__}: {error}"
        return frame, StepResult(
            position=position,
            type=step.type,
            status="failed",
            rows_in=frame.height,
            rows_out=frame.height,
            values_changed=0,
            error=reason,
        )
    return applied.frame, StepResult(
        position=position,
        type=step.type,
        status="succeeded",
        rows_in=frame.height,
        rows_out=applied.frame.height,
        values_changed=applied.values_changed,
        message=applied.message,
    )
