"""The transformation types by name, and loading declared steps against the data they will meet.

A new type is one Transformation subclass with a `type` and the `register` decorator; nothing
here changes.
"""

from collections.abc import Mapping, Sequence
from typing import Any

import polars as pl
import pydantic

from udp.errors import ConfigError
from udp.transformations.base import StepConfigError, Transformation

TRANSFORMATIONS: dict[str, type[Transformation]] = {}


def register[T: type[Transformation]](cls: T) -> T:
    """Make a transformation type declarable by its `type` name."""
    name = cls.type
    if name in TRANSFORMATIONS and TRANSFORMATIONS[name] is not cls:
        raise ValueError(f"transformation type '{name}' is already registered")
    TRANSFORMATIONS[name] = cls
    return cls


def _field(location: tuple[int | str, ...]) -> str:
    return ".".join(str(part) for part in location) or "settings"


def load_steps(definitions: Sequence[Mapping[str, Any]], schema: pl.Schema) -> list[Transformation]:
    """Every declared step, its settings validated and its columns checked against the schema
    the steps before it leave.

    Raises ConfigError naming the step, its type and the setting that is wrong.
    """
    steps: list[Transformation] = []
    for position, definition in enumerate(definitions, 1):
        step = parse_step(position, definition)
        try:
            schema = step.check(schema)
        except StepConfigError as error:
            label = f"step {position} ({step.type})"
            raise ConfigError(f"{label}: {error.field}: {error.message}") from None
        steps.append(step)
    return steps


def parse_step(position: int, definition: Mapping[str, Any]) -> Transformation:
    """One declared step with its settings validated, before any data is known.

    Raises ConfigError naming the step, its type and the setting that is wrong.
    """
    name = definition.get("type")
    if not isinstance(name, str) or name not in TRANSFORMATIONS:
        known = ", ".join(sorted(TRANSFORMATIONS))
        raise ConfigError(
            f"step {position}: type: unknown transformation type {name!r}; known: {known}"
        )
    settings = {key: value for key, value in definition.items() if key != "type"}
    try:
        return TRANSFORMATIONS[name].model_validate(settings)
    except pydantic.ValidationError as error:
        problems = "; ".join(
            f"{_field(problem['loc'])}: {problem['msg']}" for problem in error.errors()
        )
        raise ConfigError(f"step {position} ({name}): {problems}") from None
