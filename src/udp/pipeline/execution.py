"""Running a pipeline: one execution, from RAW through its steps to CLEAN, recorded as it goes.

    RAW → profile → step 1 → (profile) → step 2 → (profile) → … → validation → CLEAN → profile

V1's runner (`runner.py`) ingests a source into RAW; this prepares a dataset already in RAW, and
the two meet only at the storage layer. A run goes:

1. **Start** (`start_pipeline`): take the dataset's lock, the same one an ingest run takes, so
   two runs never share one STAGING table — a second run is refused with `PipelineBusy`, naming
   the run in progress. Store the definition (an edited one becomes the next version) and record
   the execution as running.
2. **Input**: the dataset's rows as they stand, read from RAW, which is only ever read. A full
   load's rows are those of the newest ingest run, an append's every row, a merge's the newest
   row per primary key. The platform columns are left out, and the rows are put in the order of
   their `_record_hash`, so the same RAW always gives the same frame.
3. **Steps**, in order, each over the whole frame the step before it left: each is recorded as
   running, then with its outcome and its lineage node, then profiled when the pipeline says so.
4. **Validation**: the pipeline's constraints against the result. Every outcome is stored; a
   critical one that fails fails the run.
5. **Publish**: in one transaction, the result becomes STAGING, then CLEAN, with the run's end,
   the CLEAN lineage node and the final profile.

Anything that fails — a step, a critical constraint, the database — ends the run failed with the
step and the reason, which drops STAGING and leaves CLEAN exactly as it was. The lock is held for
the whole run and freed however it ends.
"""

import json
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID, uuid7

import polars as pl
import structlog
from pydantic import BaseModel

from udp.config.pipeline import PipelineDefinition
from udp.errors import ValidationError
from udp.names import RESERVED_COLUMNS, Stage
from udp.profiling.frame import ProfileSettings, profile_frame
from udp.quality.constraints import check_frame
from udp.storage.loader import (
    INTERRUPTED,
    ExecutionStart,
    LineageNode,
    Loader,
    PipelineStages,
    PipelineVersion,
    Profile,
    RunFailure,
    RunRef,
    StepRun,
    interrupted_message,
    stage_node,
)
from udp.transformations import StepContext
from udp.transformations.base import check_and_run

log = structlog.get_logger(step="pipeline")

Clock = Callable[[], datetime]


def utc_now() -> datetime:
    return datetime.now(UTC)


class PipelineBusy(Exception):
    """The dataset is already being worked on: by a pipeline run, named, or by an ingest run."""

    def __init__(self, message: str, execution_id: UUID | None) -> None:
        super().__init__(message)
        self.execution_id = execution_id


@dataclass(frozen=True)
class Started:
    """A run recorded as running, holding its dataset's lock until `carry_out` ends it."""

    execution_id: UUID
    pipeline: PipelineDefinition
    version: PipelineVersion
    started_at: datetime


class _Failed(Exception):
    def __init__(self, failure: RunFailure, failed_step: int | None = None) -> None:
        super().__init__(failure.message)
        self.failure = failure
        self.failed_step = failed_step


def _busy(loader: Loader, source: str, dataset: str) -> PipelineBusy:
    with loader.stages() as stages:
        running = stages.running_executions(source, dataset)
        execution = stages.read_execution(running[-1]) if running else None
        version = (
            None
            if execution is None
            else stages.read_pipeline_version(execution.pipeline_id, execution.version)
        )
    named = f"{source}.{dataset}"
    if execution is None:
        return PipelineBusy(
            f"{named} is being loaded by an ingest run; start the pipeline when it has ended",
            None,
        )
    pipeline = f"pipeline '{version.name}' version {version.version}" if version else "a pipeline"
    return PipelineBusy(
        f"{named} is already being prepared by run {execution.execution_id} of {pipeline}, "
        f"started {execution.started_at.isoformat()}; start another when it has ended",
        execution.execution_id,
    )


def start_pipeline(
    loader: Loader,
    pipeline: PipelineDefinition,
    *,
    trigger: Literal["manual", "scheduled"] = "manual",
    clock: Clock = utc_now,
) -> Started:
    """Take the dataset's lock, store the definition and record the run as running.

    Raises PipelineBusy, changing nothing, when another run holds the dataset. A run still
    marked running while the lock is free was cut off — its process died — and is recorded as
    failed here, as an ingest run's is.
    """
    source, dataset = pipeline.source, pipeline.dataset.name
    if not loader.lock_dataset(source, dataset):
        raise _busy(loader, source, dataset)
    try:
        execution_id = uuid7()
        started_at = clock()
        with loader.stages() as stages:
            for stale in stages.running_executions(source, dataset):
                stages.finish_execution(
                    stale,
                    ended_at=started_at,
                    rows_in=None,
                    rows_out=None,
                    failure=RunFailure(INTERRUPTED, interrupted_message(execution_id), ""),
                )
            version = stages.save_pipeline(
                source,
                dataset,
                pipeline.name,
                pipeline.stored(),
                pipeline.step_definitions,
                started_at,
            )
            stages.start_execution(
                ExecutionStart(
                    execution_id, version.pipeline_id, version.version, trigger, started_at
                )
            )
    except BaseException:
        loader.unlock_dataset(source, dataset)
        raise
    log.info(
        "pipeline run started",
        execution_id=str(execution_id),
        pipeline=pipeline.name,
        version=version.version,
        source=source,
        dataset=dataset,
    )
    return Started(execution_id, pipeline, version, started_at)


def carry_out(loader: Loader, started: Started, *, clock: Clock = utc_now) -> PipelineRun:
    """Run a started pipeline to its end and return its record. A step that fails, a critical
    constraint that fails or any other error ends the run failed rather than raising; only a
    database that cannot even record the failure raises. The dataset's lock is freed either way.
    """
    pipeline = started.pipeline
    source, dataset = pipeline.source, pipeline.dataset.name
    progress = _Progress()
    try:
        try:
            _work(loader, started, progress, clock)
        except _Failed as failed:
            _fail(loader, started, failed.failure, failed.failed_step, progress, clock)
        except Exception as error:
            failure = RunFailure(
                type(error).__name__, str(error) or repr(error), traceback.format_exc()
            )
            _fail(loader, started, failure, None, progress, clock)
    finally:
        loader.unlock_dataset(source, dataset)
    with loader.stages() as stages:
        record = read_run(stages, started.execution_id)
    assert record is not None
    return record


def run_pipeline(
    loader: Loader,
    pipeline: PipelineDefinition,
    *,
    trigger: Literal["manual", "scheduled"] = "manual",
    clock: Clock = utc_now,
) -> PipelineRun:
    """Start a run and carry it out. Raises PipelineBusy when the dataset is taken."""
    return carry_out(
        loader, start_pipeline(loader, pipeline, trigger=trigger, clock=clock), clock=clock
    )


@dataclass
class _Progress:
    rows_in: int | None = None


def _fail(
    loader: Loader,
    started: Started,
    failure: RunFailure,
    failed_step: int | None,
    progress: _Progress,
    clock: Clock,
) -> None:
    with loader.stages() as stages:
        stages.finish_execution(
            started.execution_id,
            ended_at=clock(),
            rows_in=progress.rows_in,
            rows_out=None,
            failure=failure,
            failed_step=failed_step,
        )
    log.warning(
        "pipeline run failed",
        execution_id=str(started.execution_id),
        pipeline=started.pipeline.name,
        failed_step=failed_step,
        error=failure.message,
    )


def _profile(
    frame: pl.DataFrame,
    pipeline: PipelineDefinition,
    stage: Stage,
    run: RunRef,
    at: datetime,
    after_step: int | None = None,
) -> Profile:
    result = profile_frame(frame, ProfileSettings.for_dataset(pipeline.dataset))
    return Profile(
        source=pipeline.source,
        dataset=pipeline.dataset.name,
        stage=stage,
        run=run,
        table_rows=result.table_rows,
        result=result.model_dump(mode="json"),
        profiled_at=at,
        after_step=after_step,
    )


def read_input(stages: PipelineStages, pipeline: PipelineDefinition) -> pl.DataFrame:
    """The dataset's rows as they stand, from RAW, without the platform columns, in a fixed order.

    Raises _Failed when no ingest run has put rows in RAW yet.
    """
    source, dataset = pipeline.source, pipeline.dataset.name
    ingest_runs: list[UUID] | None = None
    if pipeline.dataset.load_mode == "full":
        newest = stages.newest_run(Stage.RAW, source, dataset)
        if newest is None or newest.ingest_run_id is None:
            raise _Failed(
                RunFailure(
                    "NoInput",
                    f"{source}.{dataset} has no rows in RAW from a succeeded ingest run; "
                    f"run `udp run {source}` first",
                    "",
                )
            )
        ingest_runs = [newest.ingest_run_id]
    frame = stages.read_raw(source, dataset, ingest_runs)
    key = list(pipeline.dataset.primary_key or ())
    if pipeline.dataset.load_mode == "merge" and key and set(key) <= set(frame.columns):
        newest_first = [name for name in ("_loaded_at", "_record_hash") if name in frame.columns]
        if newest_first:
            frame = frame.sort(
                newest_first, descending=[True, False][: len(newest_first)], nulls_last=True
            )
        frame = frame.unique(subset=key, keep="first", maintain_order=True)
    if "_record_hash" in frame.columns:
        frame = frame.sort("_record_hash", nulls_last=True, maintain_order=True)
    return frame.drop([name for name in RESERVED_COLUMNS if name in frame.columns])


def _settings(step_configuration: dict[str, Any]) -> str:
    return json.dumps(step_configuration, sort_keys=True, default=str)


def _work(loader: Loader, started: Started, progress: _Progress, clock: Clock) -> None:
    pipeline = started.pipeline
    source, dataset = pipeline.source, pipeline.dataset.name
    run = RunRef(execution_id=started.execution_id)
    profiles = pipeline.profile

    with loader.stages() as stages:
        frame = read_input(stages, pipeline)
        progress.rows_in = frame.height
        at = clock()
        stages.record_lineage(source, dataset, run, [stage_node(Stage.RAW, source, dataset)], at)
        if profiles != "none":
            stages.record_profile(_profile(frame, pipeline, Stage.RAW, run, at))

    context = StepContext(
        constraints=pipeline.constraints, primary_key=pipeline.dataset.primary_key
    )
    for position, step in enumerate(pipeline.steps, 1):
        step_started = clock()
        with loader.stages() as stages:
            stages.record_step(started.execution_id, StepRun(position, "running", step_started))
        after, result = check_and_run(frame, step, position, context)
        failed = result.status == "failed"
        ended = clock()
        with loader.stages() as stages:
            stages.record_step(
                started.execution_id,
                StepRun(
                    position,
                    result.status,
                    step_started,
                    ended,
                    rows_in=result.rows_in,
                    rows_out=result.rows_out,
                    values_changed=result.values_changed,
                    error_class="StepFailed" if failed else None,
                    error_message=result.error if failed else None,
                    script_sha256=result.script_sha256,
                    output=result.output,
                    error_line=result.error_line,
                ),
            )
            stages.record_lineage(
                source, dataset, run, [LineageNode("step", step.type, position)], ended
            )
            if not failed and profiles == "every_step":
                stages.record_profile(
                    _profile(after, pipeline, Stage.STAGING, run, ended, after_step=position)
                )
        if failed:
            configuration = _settings(step.model_dump(mode="json"))
            raise _Failed(
                RunFailure(
                    "StepFailed",
                    f"step {position} ({step.type}) with {configuration} failed: {result.error}",
                    "",
                ),
                failed_step=position,
            )
        frame = after

    _validate(loader, pipeline, frame, run, clock)

    if not frame.columns:
        raise _Failed(RunFailure("NoColumns", "the steps left no columns to publish", ""))
    final = _profile(frame, pipeline, Stage.CLEAN, run, clock()) if profiles != "none" else None
    with loader.stages() as stages:
        stages.write_staging(source, dataset, frame)
        ended = clock()
        stages.finish_execution(
            started.execution_id, ended_at=ended, rows_in=progress.rows_in, rows_out=frame.height
        )
        stages.record_lineage(
            source, dataset, run, [stage_node(Stage.CLEAN, source, dataset)], ended
        )
        if final is not None:
            stages.record_profile(final)
    log.info(
        "pipeline run succeeded",
        execution_id=str(started.execution_id),
        pipeline=pipeline.name,
        rows_in=progress.rows_in,
        rows_out=frame.height,
    )


def _validate(
    loader: Loader, pipeline: PipelineDefinition, frame: pl.DataFrame, run: RunRef, clock: Clock
) -> None:
    """Check the pipeline's constraints against the result and store every outcome; raise
    _Failed when one that is critical fails, or when one cannot be checked at all."""
    if not pipeline.constraints:
        return
    checked_at = clock()
    try:
        outcomes = check_frame(frame, pipeline.constraints, pipeline.dataset.primary_key)
    except ValidationError as error:
        raise _Failed(RunFailure("ValidationError", f"validation: {error}", "")) from None
    results = [
        outcome.result(pipeline.source, pipeline.dataset.name, Stage.STAGING, run, checked_at)
        for outcome in outcomes
    ]
    with loader.stages() as stages:
        stages.record_constraint_results(results)
    critical = [
        outcome for outcome in outcomes if outcome.constraint.critical and not outcome.passed
    ]
    if critical:
        broken = "; ".join(
            f"constraint {outcome.position} ({outcome.constraint.constraint} on "
            f"{', '.join(outcome.constraint.columns_checked)}): {outcome.message}"
            for outcome in critical
        )
        raise _Failed(RunFailure("CriticalConstraintFailed", f"validation: critical {broken}", ""))


# --- the record of a run, as the CLI prints it and the API returns it ------------------------

StepState = Literal["not_run", "running", "succeeded", "failed"]


class StepRecord(BaseModel):
    """One step of the version the run ran: its settings, and what it did in this run."""

    position: int
    type: str
    configuration: dict[str, Any]
    status: StepState
    started_at: datetime | None = None
    ended_at: datetime | None = None
    duration_seconds: float | None = None
    rows_in: int | None = None
    rows_out: int | None = None
    values_changed: int | None = None
    error: str | None = None
    script_sha256: str | None = None
    output: str | None = None
    error_line: int | None = None


class ProfileRecord(BaseModel):
    """One profile the run took: where, and its totals. The whole profile is stored by id."""

    profile_id: int
    stage: Literal["raw", "staging", "clean"]
    after_step: int | None
    table_rows: int
    missing_values: int
    invalid_values: int
    outliers: int
    duplicates: int
    profiled_at: datetime


class ValidationRecord(BaseModel):
    """One constraint checked against the result before it became CLEAN."""

    position: int
    constraint: str
    columns: list[str]
    critical: bool
    passed: bool
    failing_rows: int | None
    failing_values: int | None
    message: str


class LineageRecord(BaseModel):
    """One place the data passed through, in order; a step with the settings it ran with."""

    kind: Literal["source", "raw", "step", "clean"]
    name: str
    step_position: int | None = None
    configuration: dict[str, Any] | None = None


class PipelineRun(BaseModel):
    """One run of one pipeline version: what ran, over what, how it ended, and what it measured.

    `failed_step` is the step it failed at; a run that failed elsewhere — its input, validation
    or publishing — has none, and `error` says where.
    """

    execution_id: UUID
    pipeline: str
    pipeline_id: int
    version: int
    source: str
    dataset: str
    trigger: str
    status: Literal["running", "succeeded", "failed"]
    started_at: datetime
    ended_at: datetime | None
    rows_in: int | None
    rows_out: int | None
    failed_step: int | None
    error_class: str | None
    error: str | None
    steps: list[StepRecord]
    profiles: list[ProfileRecord]
    validation: list[ValidationRecord]
    lineage: list[LineageRecord]


def read_run(stages: PipelineStages, execution_id: UUID) -> PipelineRun | None:
    """A run's whole record, read back from what it stored; None when there is no such run."""
    execution = stages.read_execution(execution_id)
    if execution is None:
        return None
    version = stages.read_pipeline_version(execution.pipeline_id, execution.version)
    assert version is not None, "an execution always names a stored version"
    run = RunRef(execution_id=execution_id)
    done = {step.position: step for step in execution.steps}
    steps = []
    for position, definition in enumerate(version.steps, 1):
        step = done.get(position)
        steps.append(
            StepRecord(
                position=position,
                type=definition.step_type,
                configuration=definition.configuration,
                status="not_run",
            )
            if step is None
            else StepRecord(
                position=position,
                type=definition.step_type,
                configuration=definition.configuration,
                status=step.status,
                started_at=step.started_at,
                ended_at=step.ended_at,
                duration_seconds=(
                    None
                    if step.ended_at is None
                    else (step.ended_at - step.started_at).total_seconds()
                ),
                rows_in=step.rows_in,
                rows_out=step.rows_out,
                values_changed=step.values_changed,
                error=step.error_message,
                script_sha256=step.script_sha256,
                output=step.output,
                error_line=step.error_line,
            )
        )
    profiles = [
        ProfileRecord(
            profile_id=stored.profile_id,
            stage=Stage(stored.profile.stage).value,
            after_step=stored.profile.after_step,
            table_rows=stored.profile.table_rows,
            missing_values=stored.profile.result.get("missing_values", 0),
            invalid_values=stored.profile.result.get("invalid_values", 0),
            outliers=stored.profile.result.get("outliers", 0),
            duplicates=stored.profile.result.get("duplicates", 0),
            profiled_at=stored.profile.profiled_at,
        )
        for stored in stages.read_profiles(execution.source, execution.dataset, run=run)
    ]
    validation = [
        ValidationRecord(
            position=result.position,
            constraint=result.constraint_type,
            columns=list(result.columns),
            critical=result.critical,
            passed=result.passed,
            failing_rows=result.failing_rows,
            failing_values=result.failing_values,
            message=result.message,
        )
        for result in stages.read_constraint_results(execution.source, execution.dataset, run=run)
    ]
    lineage = [
        LineageRecord(
            kind=node.kind,
            name=node.name,
            step_position=node.step_position,
            configuration=(
                None
                if node.step_position is None
                else version.steps[node.step_position - 1].configuration
            ),
        )
        for node in stages.read_lineage(run)
    ]
    return PipelineRun(
        execution_id=execution_id,
        pipeline=version.name,
        pipeline_id=execution.pipeline_id,
        version=execution.version,
        source=execution.source,
        dataset=execution.dataset,
        trigger=execution.trigger,
        status=execution.status,  # type: ignore[arg-type]
        started_at=execution.started_at,
        ended_at=execution.ended_at,
        rows_in=execution.rows_in,
        rows_out=execution.rows_out,
        failed_step=execution.failed_step,
        error_class=execution.error_class,
        error=execution.error_message,
        steps=steps,
        profiles=profiles,
        validation=validation,
        lineage=lineage,
    )


def _rows(count: int | None) -> str:
    return "?" if count is None else f"{count:,}"


def describe_run(run: PipelineRun) -> str:
    """The run as text a person reads in a terminal: how it ended, then each step, the
    constraints and the profiles."""
    lines = [
        f"Run {run.execution_id} of pipeline '{run.pipeline}' version {run.version} "
        f"over {run.source}.{run.dataset}: {run.status}",
        f"Started {run.started_at.isoformat()}"
        + ("" if run.ended_at is None else f", ended {run.ended_at.isoformat()}")
        + f"; {_rows(run.rows_in)} rows in, {_rows(run.rows_out)} rows out",
    ]
    if run.error is not None:
        lines.append(f"Error ({run.error_class}): {run.error}")
    lines.append("")
    lines.append("Steps:" if run.steps else "Steps: none")
    for step in run.steps:
        line = f"  {step.position}. {step.type}: {step.status.replace('_', ' ')}"
        if step.status in ("succeeded", "failed"):
            line += (
                f", {_rows(step.rows_in)} → {_rows(step.rows_out)} rows, "
                f"{_rows(step.values_changed)} values changed, {step.duration_seconds or 0:.2f}s"
            )
        if step.error is not None:
            line += f" — {step.error}"
        lines.append(line)
    if run.validation:
        holding = sum(check.passed for check in run.validation)
        lines.append(f"Validation: {holding} of {len(run.validation)} constraints hold")
        for check in run.validation:
            critical = ", critical" if check.critical else ""
            lines.append(
                f"  {check.position}. {check.constraint} ({', '.join(check.columns)}){critical}: "
                f"{check.message}"
            )
    if run.profiles:
        taken = ", ".join(
            f"{profile.stage}"
            + ("" if profile.after_step is None else f" after step {profile.after_step}")
            + f" ({_rows(profile.table_rows)} rows, profile {profile.profile_id})"
            for profile in run.profiles
        )
        lines.append(f"Profiles: {taken}")
    return "\n".join(lines)
