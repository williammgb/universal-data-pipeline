"""A pipeline as declared: the dataset it prepares, the steps it runs and what it checks.

Each pipeline is one file, `pipelines/<name>.yaml`, in the folder `sources/` is in:

    source: demo_csv            # a folder under sources/
    dataset: customers          # a dataset in that source's source.yaml
    profile: ends               # none | ends | every_step; ends when left out
    constraints:                # left out: the dataset's own constraints from source.yaml
      - constraint: not_null
        column: customer_id
        critical: true
    steps:                      # in order; none at all is allowed
      - type: normalize_values
        columns: [city]
        trim: true

`profile` says which profiles a run takes: `ends` profiles the rows the run starts from and the
CLEAN table it ends with, `every_step` also profiles the data after each step, and `none` takes
no profile. A profile reads every value, so one after every step of a long pipeline over a large
dataset costs that many passes over the data.

The definition a run stores is the file with everything resolved — the constraints it checked
and every step with every setting, defaults included — so a run always says what it ran.
"""

import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pydantic
import yaml
from pydantic import BaseModel, ConfigDict

from udp.config.constraints import Constraint
from udp.config.source import SourceConfig, load_source
from udp.connectors.base import DatasetBase
from udp.errors import ConfigError
from udp.names import name_problem
from udp.storage.loader import StepDefinition
from udp.transformations import Transformation
from udp.transformations.registry import TRANSFORMATIONS, parse_step

PIPELINES_DIR = "pipelines"
PIPELINE_SUFFIX = ".yaml"

ProfileChoice = Literal["none", "ends", "every_step"]

# Where a problem with a draft is, the way the builder places it on a block: ("steps", 2, "method")
# or ("constraints", 1, "column"), counted from 1 as a pipeline's steps are.
Where = tuple[str | int, ...]

_CONSTRAINT: pydantic.TypeAdapter[Constraint] = pydantic.TypeAdapter(Constraint)


class PipelineFile(BaseModel):
    """The file as written, checked for shape; the steps are checked one by one afterwards."""

    model_config = ConfigDict(extra="forbid")

    source: str
    dataset: str
    profile: ProfileChoice = "ends"
    constraints: list[Constraint] | None = None
    steps: list[dict[str, Any]] = []


@dataclass(frozen=True)
class PipelineDefinition:
    """A pipeline ready to run: its dataset as the source declares it, and its steps parsed."""

    name: str
    source: str
    dataset: DatasetBase
    profile: ProfileChoice
    constraints: tuple[Constraint, ...]
    steps: tuple[Transformation, ...]

    @property
    def step_definitions(self) -> tuple[StepDefinition, ...]:
        """Each step as stored: its type and every setting it runs with."""
        return tuple(StepDefinition(step.type, step.model_dump(mode="json")) for step in self.steps)

    def stored(self) -> dict[str, Any]:
        """The whole definition as one version stores it."""
        return {
            "source": self.source,
            "dataset": self.dataset.name,
            "profile": self.profile,
            "constraints": [constraint.model_dump(mode="json") for constraint in self.constraints],
            "steps": [
                {"type": step.step_type, **step.configuration} for step in self.step_definitions
            ],
        }


def pipelines_dir(sources_dir: Path) -> Path:
    """Where pipeline files are: `pipelines/`, next to `sources/`."""
    return sources_dir.parent / PIPELINES_DIR


def pipeline_path(sources_dir: Path, name: str) -> Path:
    return pipelines_dir(sources_dir) / f"{name}{PIPELINE_SUFFIX}"


def pipeline_exists(sources_dir: Path, name: str) -> bool:
    return name_problem(name) is None and pipeline_path(sources_dir, name).is_file()


def read_pipeline_file(sources_dir: Path, name: str) -> PipelineFile:
    """Read and check pipelines/<name>.yaml for shape. Every problem raises ConfigError."""
    path = pipeline_path(sources_dir, name)
    shown = path.as_posix()
    problem = name_problem(name)
    if problem is not None:
        raise ConfigError(f"{shown}: pipeline name '{name}' {problem}")
    if not path.is_file():
        raise ConfigError(f"{shown}: file not found")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as error:
        mark = getattr(error, "problem_mark", None)
        where = f"line {mark.line + 1}: " if mark is not None else ""
        reason = getattr(error, "problem", error)
        raise ConfigError(f"{shown}: {where}invalid YAML: {reason}") from error
    if not isinstance(data, dict):
        raise ConfigError(f"{shown}: expected a mapping with 'source', 'dataset' and 'steps'")
    try:
        return PipelineFile.model_validate(data)
    except pydantic.ValidationError as error:
        problems = [
            f"{shown}: {'.'.join(str(part) for part in problem['loc'])}: {problem['msg']}"
            for problem in error.errors()
        ]
        raise ConfigError("\n".join(problems)) from None


def resolve_pipeline(
    name: str, written: PipelineFile, config: SourceConfig[Any, Any], shown: str
) -> PipelineDefinition:
    """The file against its source's configuration: the dataset must be there, and every step's
    settings must be valid. Whether a step's columns are there is only known once the data is,
    so that is checked by the run, against the frame each step meets."""
    dataset = next((item for item in config.datasets if item.name == written.dataset), None)
    if dataset is None:
        raise ConfigError(
            f"{shown}: dataset: source '{written.source}' has no dataset '{written.dataset}'"
        )
    try:
        steps = tuple(
            parse_step(position, definition) for position, definition in enumerate(written.steps, 1)
        )
    except ConfigError as error:
        raise ConfigError(f"{shown}: steps: {error}") from None
    constraints = written.constraints if written.constraints is not None else dataset.constraints
    return PipelineDefinition(
        name=name,
        source=written.source,
        dataset=dataset,
        profile=written.profile,
        constraints=tuple(constraints),
        steps=steps,
    )


def draft_problems(
    constraints: Sequence[Any], steps: Sequence[Mapping[str, Any]]
) -> list[tuple[Where, str]]:
    """Every problem with a pipeline's constraints and steps as drafted, each with where it is.

    These are the checks loading a pipeline makes — each constraint's shape, each step's type
    and settings, as `parse_step` reads them — reported one by one rather than as one message,
    so a draft with none of them loads. Whether a column is there is a question for the run.
    A problem with a whole constraint or step is put on its `constraint` or `type` field.
    """
    problems: list[tuple[Where, str]] = []
    for position, constraint in enumerate(constraints, 1):
        try:
            _CONSTRAINT.validate_python(constraint)
        except pydantic.ValidationError as error:
            kind = constraint.get("constraint") if isinstance(constraint, Mapping) else None
            for problem in error.errors():
                # A tagged union names the tag first, ("min", "value"); the field is what follows.
                place = problem["loc"][1:] if problem["loc"][:1] == (kind,) else problem["loc"]
                where = ("constraints", position, *(place or ("constraint",)))
                problems.append((where, problem["msg"]))
    for position, step in enumerate(steps, 1):
        name = step.get("type")
        if not isinstance(name, str) or name not in TRANSFORMATIONS:
            known = ", ".join(sorted(TRANSFORMATIONS))
            problems.append((("steps", position, "type"), f"unknown type {name!r}; known: {known}"))
            continue
        settings = {key: value for key, value in step.items() if key != "type"}
        try:
            TRANSFORMATIONS[name].model_validate(settings)
        except pydantic.ValidationError as error:
            problems.extend(
                (("steps", position, *(problem["loc"] or ("type",))), problem["msg"])
                for problem in error.errors()
            )
    return problems


def load_pipeline(
    sources_dir: Path,
    name: str,
    env: Mapping[str, str] = os.environ,
    overrides: Mapping[str, Mapping[str, Any]] = {},
) -> PipelineDefinition:
    """Read pipelines/<name>.yaml and the source it names, with the source's stored edits laid
    over its file as `load_source` does. Every problem raises ConfigError."""
    written = read_pipeline_file(sources_dir, name)
    config = load_source(sources_dir, written.source, env, overrides)
    return resolve_pipeline(name, written, config, pipeline_path(sources_dir, name).as_posix())
