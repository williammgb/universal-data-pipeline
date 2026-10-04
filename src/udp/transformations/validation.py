"""Validation: the dataset's constraints as a step.

It checks the step's own constraints, or the dataset's from the step context when it names none,
and counts the rows that break any of them. `on_invalid: drop` removes those rows. A critical
constraint that fails fails the step when `stop_on_critical` is set, which is the default, so the
pipeline can stop there. It never changes a value.
"""

from typing import Literal

import polars as pl

from udp.config.constraints import Constraint
from udp.errors import ValidationError
from udp.quality.constraints import breaks, check_frame
from udp.transformations.base import (
    Applied,
    StepConfigError,
    StepContext,
    StepFailed,
    Transformation,
)
from udp.transformations.registry import register


@register
class Validate(Transformation):
    type = "validate"
    constraints: list[Constraint] | None = None
    on_invalid: Literal["keep", "drop"] = "keep"
    stop_on_critical: bool = True

    def check(self, schema: pl.Schema) -> pl.Schema:
        if self.constraints:
            try:
                check_frame(pl.DataFrame(schema=schema), self.constraints)
            except ValidationError as error:
                raise StepConfigError("constraints", str(error)) from None
        return schema

    def apply(self, frame: pl.DataFrame, context: StepContext) -> Applied:
        constraints = self.constraints if self.constraints is not None else context.constraints
        if not constraints:
            return Applied(frame, values_changed=0, message="no constraints to check")
        outcomes = check_frame(frame, constraints, context.primary_key)
        failed = [outcome for outcome in outcomes if not outcome.passed]
        critical = [outcome for outcome in failed if outcome.constraint.critical]
        if critical and self.stop_on_critical:
            first = critical[0]
            raise StepFailed(
                f"critical constraint {first.position} ({first.constraint.constraint}) "
                f"failed: {first.message}"
            )
        invalid = pl.DataFrame([breaks(frame, c).alias(str(i)) for i, c in enumerate(constraints)])
        rows = invalid.select(pl.any_horizontal(pl.all())).to_series()
        count = int(rows.sum())
        broken = "; ".join(
            f"constraint {outcome.position} ({outcome.constraint.constraint}): {outcome.message}"
            for outcome in failed
        )
        message = f"{count} invalid rows" + (f" — {broken}" if broken else "")
        result = frame.filter(~rows) if self.on_invalid == "drop" else frame
        return Applied(result, values_changed=0, message=message)
