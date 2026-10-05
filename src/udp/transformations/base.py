"""What every transformation is: a named type, validated settings, and a step that runs on its own.

A transformation is a pydantic model holding its settings. `check` is the load-time test of
those settings against the columns the step will meet; `apply` changes a frame and says how many
values it changed. Steps run over a whole frame — the dataset's materialised stage table — never
chunk by chunk, so a mean or a percentile is taken over every row. They take and return frames
and never write a table, so none of them can touch RAW.
"""

import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, ClassVar, Literal

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


OnFailure = Literal["stop", "continue"]


@dataclass(frozen=True)
class ScriptRun:
    """What a python step's script left behind: which code ran, what it printed, and the line of
    the script it failed on, when it failed there."""

    sha256: str
    output: str
    error_line: int | None = None


class StepFailed(Exception):
    """A step could not do what it was asked; the frame is left as it was."""

    def __init__(self, message: str, script: ScriptRun | None = None) -> None:
        super().__init__(message)
        self.script = script


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
    script: ScriptRun | None = None


class StepResult(BaseModel):
    """One step's outcome, as a person reads it. A python step also says which code ran
    (`script_sha256`), what it printed, and the script line it failed on."""

    position: int
    type: str
    status: StepStatus
    rows_in: int
    rows_out: int
    values_changed: int
    duration_seconds: float = 0.0
    error: str | None = None
    message: str | None = None
    script_sha256: str | None = None
    output: str | None = None
    error_line: int | None = None


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
    started = time.monotonic()
    try:
        applied = step.apply(frame, context or StepContext())
    except Exception as error:
        reason = str(error) if isinstance(error, StepFailed) else f"{type(error).__name__}: {error}"
        script = error.script if isinstance(error, StepFailed) else None
        return frame, StepResult(
            position=position,
            type=step.type,
            status="failed",
            rows_in=frame.height,
            rows_out=frame.height,
            values_changed=0,
            duration_seconds=time.monotonic() - started,
            error=reason,
            **_script_fields(script),
        )
    return applied.frame, StepResult(
        position=position,
        type=step.type,
        status="succeeded",
        rows_in=frame.height,
        rows_out=applied.frame.height,
        values_changed=applied.values_changed,
        duration_seconds=time.monotonic() - started,
        message=applied.message,
        **_script_fields(applied.script),
    )


def _script_fields(script: ScriptRun | None) -> dict[str, Any]:
    if script is None:
        return {}
    return {
        "script_sha256": script.sha256,
        "output": script.output,
        "error_line": script.error_line,
    }


def run_steps(
    frame: pl.DataFrame,
    steps: Sequence[Transformation],
    context: StepContext | None = None,
    on_failure: OnFailure = "stop",
) -> tuple[pl.DataFrame, list[StepResult]]:
    """Run steps in order, each on the frame the steps before it left.

    Each step is checked again against that frame first, because a python step can change the
    columns in ways nothing knew when the steps were loaded. A step that fails leaves the frame as
    it was; `stop` ends the run there, `continue` hands that frame to the next step. Either way
    the results of the steps before it are kept.
    """
    results: list[StepResult] = []
    for position, step in enumerate(steps, 1):
        try:
            step.check(frame.schema)
        except StepConfigError as error:
            result = StepResult(
                position=position,
                type=step.type,
                status="failed",
                rows_in=frame.height,
                rows_out=frame.height,
                values_changed=0,
                error=f"{error.field}: {error.message}",
            )
        else:
            frame, result = run_step(frame, step, position, context)
        results.append(result)
        if result.status == "failed" and on_failure == "stop":
            break
    return frame, results
