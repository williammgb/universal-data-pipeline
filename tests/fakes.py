import json
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import asdict
from datetime import date, datetime
from typing import Any
from uuid import UUID

import polars as pl

from udp.errors import LoadError
from udp.names import Stage, stage_table
from udp.storage.loader import (
    INTERRUPTED,
    Column,
    ColumnChanges,
    ConfigCopy,
    ConstraintResult,
    DatasetState,
    Execution,
    ExecutionStart,
    LineageNode,
    LoadResult,
    PipelineVersion,
    Profile,
    RunFailure,
    RunFindings,
    RunRef,
    RunStart,
    StepDefinition,
    StepRun,
    StoredProfile,
    check_chain,
    column_changes,
    interrupted_message,
    table_columns,
)

_DROPPED = object()


def raw_table(source: str, dataset: str) -> str:
    """The key of a dataset's RAW table in `MemoryLoader.raw`: its schema-qualified name."""
    return ".".join(stage_table(Stage.RAW, source, dataset))


class MemoryCatalog:
    """The catalog's configuration reads and writes, without a database.

    Only what the configuration tab's two routes ask for: the stored overrides, the history,
    and what the last load recorded. Everything else the API reads still needs Postgres.
    """

    def __init__(
        self,
        state: DatasetState | None = None,
        columns: Sequence[tuple[str, str]] = (),
    ) -> None:
        self.saved: list[dict[str, Any]] = []
        self.state = state
        self.columns = list(columns)

    def overrides(self, source: str) -> dict[str, dict[str, Any]]:
        in_force: dict[str, dict[str, Any]] = {}
        for edit in self.saved:
            if edit["source"] == source:
                in_force[edit["dataset"]] = edit["override"]
        return {name: override for name, override in in_force.items() if override}

    def edits(self, source: str, dataset: str) -> list[Any]:
        from udp.api.models import ConfigEdit

        return [
            ConfigEdit(changed=edit["changed"], changed_at=edit["changed_at"])
            for edit in reversed(self.saved)
            if (edit["source"], edit["dataset"]) == (source, dataset)
        ]

    def save_override(
        self,
        source: str,
        dataset: str,
        override: dict[str, Any],
        changed: dict[str, Any],
        changed_at: datetime,
    ) -> None:
        self.saved.append(
            {
                "source": source,
                "dataset": dataset,
                "override": override,
                "changed": changed,
                "changed_at": changed_at,
            }
        )

    def loaded_state(
        self, source: str, dataset: str
    ) -> tuple[DatasetState | None, list[tuple[str, str]]]:
        return self.state, self.columns

    def close(self) -> None:
        return None


class MemoryTransaction:
    def __init__(self, loader: MemoryLoader) -> None:
        self._loader = loader
        self.tables: dict[str, Any] = {}
        self.raw: dict[str, Any] = {}
        self._loaded: pl.DataFrame | None = None
        self.states: dict[tuple[str, str], DatasetState] = {}
        self.versions: dict[tuple[str, str], list[list[Column]]] = {}
        self.run_updates: dict[UUID, dict[str, Any]] = {}
        self.quarantine: list[dict[str, Any]] = []
        self.quality_results: list[dict[str, Any]] = []
        self.lineage: dict[RunRef, list[LineageNode]] = {}

    def table(self, name: str) -> pl.DataFrame | None:
        found = self.tables.get(name, self._loader.tables.get(name))
        return None if found is _DROPPED else found

    def _batch(
        self, table: str, chunks: Iterable[pl.DataFrame]
    ) -> tuple[pl.DataFrame | None, pl.DataFrame, ColumnChanges]:
        frames = list(chunks)
        if not frames:
            raise LoadError(f"no data to load into datasets.{table}")
        names = frames[0].columns
        if any(frame.columns != names for frame in frames):
            raise LoadError(f"chunk columns differ from {names}")
        batch = pl.concat(frames)
        table_columns(batch)
        existing = self.table(table)
        # What Postgres leaves in its stage table: the batch, with the table's columns.
        self._loaded = self._widen(existing, batch)
        if existing is None:
            return None, batch, ColumnChanges((), ())
        return existing, batch, column_changes(table, table_columns(existing), batch.schema)

    @staticmethod
    def _widen(existing: pl.DataFrame | None, batch: pl.DataFrame) -> pl.DataFrame:
        """The batch with the table's column order, adding the table's missing columns."""
        if existing is None:
            return batch
        empty = pl.concat([existing.head(0), batch.head(0)], how="diagonal_relaxed")
        return pl.concat([empty, batch], how="diagonal_relaxed").select(empty.columns)

    def replace_table(self, table: str, chunks: Iterable[pl.DataFrame]) -> LoadResult:
        existing, batch, changes = self._batch(table, chunks)
        self.tables[table] = self._widen(existing, batch)
        return LoadResult(batch.height, changes.added, changes.missing)

    def append_rows(self, table: str, chunks: Iterable[pl.DataFrame]) -> LoadResult:
        existing, batch, changes = self._batch(table, chunks)
        if existing is None:
            self.tables[table] = batch
        else:
            self.tables[table] = pl.concat([existing, batch], how="diagonal_relaxed")
        return LoadResult(batch.height, changes.added, changes.missing)

    def merge_rows(
        self,
        table: str,
        chunks: Iterable[pl.DataFrame],
        *,
        primary_key: Sequence[str],
        watermark: str,
    ) -> LoadResult:
        existing, batch, changes = self._batch(table, chunks)
        key = list(primary_key)
        latest = (
            batch.with_row_index("__order")
            .sort(
                [*key, watermark, "__order"],
                descending=[False] * len(key) + [True, True],
                nulls_last=True,
            )
            .unique(subset=key, keep="first", maintain_order=True)
            .drop("__order")
        )
        if existing is None:
            self.tables[table] = latest
            return LoadResult(latest.height, changes.added, changes.missing)
        if latest.height == 0:
            if changes.added:
                self.tables[table] = pl.concat([existing, latest], how="diagonal_relaxed")
            return LoadResult(0, changes.added, changes.missing)

        stored = existing.select([*key, "_record_hash"]).rename({"_record_hash": "__stored"})
        joined = latest.join(stored, on=key, how="left")
        inserts = joined.filter(pl.col("__stored").is_null()).drop("__stored")
        updates = joined.filter(
            pl.col("__stored").is_not_null() & (pl.col("__stored") != pl.col("_record_hash"))
        ).drop("__stored")
        written = inserts.height + updates.height
        if written == 0 and not changes.added:
            return LoadResult(0, changes.added, changes.missing)

        kept_columns = [name for name in existing.columns if name not in updates.columns]
        untouched = existing.join(updates.select(key), on=key, how="anti")
        updated = existing.select([*key, *kept_columns]).join(updates, on=key, how="inner")
        merged = pl.concat([untouched, updated, inserts], how="diagonal_relaxed")
        self.tables[table] = merged.select(self._widen(existing, latest).columns)
        return LoadResult(written, changes.added, changes.missing)

    def drop_table(self, table: str) -> None:
        self.tables[table] = _DROPPED

    def drop_raw(self, source: str, dataset: str) -> None:
        self.raw[raw_table(source, dataset)] = _DROPPED

    def append_raw(self, source: str, dataset: str) -> int:
        if self._loaded is None:
            raise LoadError(f"nothing was loaded for {source}.{dataset} to add to RAW")
        name = raw_table(source, dataset)
        existing = self.raw.get(name, self._loader.raw.get(name))
        if existing is None or existing is _DROPPED:
            self.raw[name] = self._loaded
        else:
            self.raw[name] = pl.concat([existing, self._loaded], how="diagonal_relaxed")
        return self._loaded.height

    def read_state(self, source: str, dataset: str) -> DatasetState | None:
        key = (source, dataset)
        return self.states.get(key, self._loader.states.get(key))

    def save_state(self, state: DatasetState) -> None:
        self.states[(state.source, state.dataset)] = state

    def record_columns(
        self, table: str, source: str, dataset: str, run_id: UUID, recorded_at: datetime
    ) -> int | None:
        frame = self.table(table)
        if frame is None:
            return None
        key = (source, dataset)
        history = [*self._loader.versions.get(key, []), *self.versions.get(key, [])]
        columns = table_columns(frame)
        if history and history[-1] == columns:
            return None
        self.versions.setdefault(key, []).append(columns)
        return len(history) + 1

    def _existing(self, table: str) -> pl.DataFrame:
        frame = self.table(table)
        if frame is None:
            raise LoadError(f"datasets.{table} does not exist")
        return frame

    def table_rows(self, table: str) -> int:
        return self._existing(table).height

    def duplicate_rows(self, table: str, columns: Sequence[str]) -> int:
        keyed = self._existing(table).select(columns).drop_nulls()
        sizes = keyed.group_by(columns).len()
        return int(sizes.filter(pl.col("len") > 1)["len"].sum())

    def newest_value(self, table: str, column: str) -> date | datetime | None:
        value = self._existing(table)[column].max()
        return value if isinstance(value, date) else None

    def previous_table_rows(self, source: str, dataset: str) -> int | None:
        for result in reversed(self._loader.quality_results):
            run = self._loader.runs[result["run_id"]]
            if (
                (result["source"], result["dataset"]) == (source, dataset)
                and run["status"] == "succeeded"
                and result["table_rows"] is not None
            ):
                return int(result["table_rows"])
        return None

    def record_findings(self, run_id: UUID, findings: RunFindings, recorded_at: datetime) -> None:
        for frame in findings.quarantine:
            for reason, record in frame.iter_rows():
                self.quarantine.append(
                    {
                        "run_id": run_id,
                        "source": findings.source,
                        "dataset": findings.dataset,
                        "reason": reason,
                        "record": json.loads(record),
                        "quarantined_at": recorded_at,
                    }
                )
        for result in findings.results:
            self.quality_results.append(
                {
                    "run_id": run_id,
                    "source": findings.source,
                    "dataset": findings.dataset,
                    **asdict(result),
                    "checked_at": recorded_at,
                }
            )
        self.run_updates.setdefault(run_id, {})["rows_quarantined"] = findings.quarantined_rows

    def succeed_run(
        self, run_id: UUID, *, ended_at: datetime, rows_extracted: int, rows_loaded: int
    ) -> None:
        run = self._loader.runs.get(run_id)
        if run is None or run["status"] != "running":
            raise LoadError(f"run {run_id} is not a running run")
        self.run_updates.setdefault(run_id, {}).update(
            status="succeeded",
            ended_at=ended_at,
            rows_extracted=rows_extracted,
            rows_loaded=rows_loaded,
        )

    def record_lineage(
        self,
        source: str,
        dataset: str,
        run: RunRef,
        nodes: Sequence[LineageNode],
        recorded_at: datetime,
    ) -> None:
        recorded = [*self._loader.lineage.get(run, ()), *self.lineage.get(run, ())]
        check_chain(source, dataset, run, [*recorded, *nodes])
        self.lineage.setdefault(run, []).extend(nodes)


def _stage_name(stage: Stage, source: str, dataset: str) -> str:
    return ".".join(stage_table(stage, source, dataset))


class MemoryStages:
    """In-memory stand-in for PostgresStages, held to the same contract for what a pipeline run
    reads and writes. Everything is written at once: there is no transaction to roll back."""

    def __init__(self, loader: MemoryLoader) -> None:
        self._loader = loader

    def save_pipeline(
        self,
        source: str,
        dataset: str,
        name: str,
        definition: dict[str, Any],
        steps: Sequence[StepDefinition],
        created_at: datetime,
    ) -> PipelineVersion:
        stage_table(Stage.CLEAN, source, dataset)
        newest = self.read_pipeline(source, dataset, name)
        stored_steps = tuple(
            StepDefinition(step.step_type, json.loads(json.dumps(step.configuration)))
            for step in steps
        )
        stored = json.loads(json.dumps(definition))
        if newest is not None and (newest.definition, newest.steps) == (stored, stored_steps):
            return newest
        pipelines = self._loader.pipelines
        pipeline_id = pipelines.setdefault((source, dataset, name), len(pipelines) + 1)
        version = PipelineVersion(
            pipeline_id=pipeline_id,
            source=source,
            dataset=dataset,
            name=name,
            version=1 if newest is None else newest.version + 1,
            definition=stored,
            steps=stored_steps,
            created_at=created_at,
        )
        self._loader.pipeline_versions[(pipeline_id, version.version)] = version
        return version

    def read_pipeline(
        self, source: str, dataset: str, name: str, version: int | None = None
    ) -> PipelineVersion | None:
        pipeline_id = self._loader.pipelines.get((source, dataset, name))
        found = [
            stored
            for (owner, number), stored in self._loader.pipeline_versions.items()
            if owner == pipeline_id and (version is None or number == version)
        ]
        return max(found, key=lambda stored: stored.version, default=None)

    def read_pipeline_version(self, pipeline_id: int, version: int) -> PipelineVersion | None:
        return self._loader.pipeline_versions.get((pipeline_id, version))

    def _owner(self, execution_id: UUID) -> PipelineVersion:
        execution = self._loader.executions[execution_id]
        return self._loader.pipeline_versions[(execution["pipeline_id"], execution["version"])]

    def running_executions(self, source: str, dataset: str) -> list[UUID]:
        running = [
            (execution["started_at"], execution_id)
            for execution_id, execution in self._loader.executions.items()
            if execution["status"] == "running"
            and (self._owner(execution_id).source, self._owner(execution_id).dataset)
            == (source, dataset)
        ]
        return [execution_id for _, execution_id in sorted(running)]

    def newest_run(self, stage: Stage, source: str, dataset: str) -> RunRef | None:
        if Stage(stage) is Stage.RAW:
            raw = self._loader.raw.get(raw_table(source, dataset))
            if raw is None:
                return None
            present = set(raw.get_column("_run_id").to_list())
            runs = [
                run
                for run in self._loader.runs.values()
                if (run["source"], run["dataset"], run["status"]) == (source, dataset, "succeeded")
                and str(run["run_id"]) in present
            ]
            newest = max(runs, key=lambda run: run["started_at"], default=None)
            return None if newest is None else RunRef(ingest_run_id=newest["run_id"])
        wanted = "running" if Stage(stage) is Stage.STAGING else "succeeded"
        executions = [
            (execution["started_at"], execution_id)
            for execution_id, execution in self._loader.executions.items()
            if execution["status"] == wanted
            and (self._owner(execution_id).source, self._owner(execution_id).dataset)
            == (source, dataset)
        ]
        return RunRef(execution_id=max(executions)[1]) if executions else None

    def read_raw(
        self, source: str, dataset: str, ingest_runs: Sequence[UUID] | None = None
    ) -> pl.DataFrame:
        raw = self._loader.raw.get(raw_table(source, dataset))
        if raw is None:
            raise LoadError(f"{source}.{dataset} has no RAW table: run `udp run {source}` first")
        if ingest_runs is None:
            return raw.clone()
        return raw.filter(pl.col("_run_id").is_in([str(run) for run in ingest_runs]))

    def write_staging(self, source: str, dataset: str, frame: pl.DataFrame) -> None:
        if not frame.columns:
            raise LoadError(f"nothing to write to STAGING of {source}.{dataset}: no columns")
        self._loader.staging[_stage_name(Stage.STAGING, source, dataset)] = frame.clone()

    def start_execution(self, start: ExecutionStart) -> None:
        if (start.pipeline_id, start.version) not in self._loader.pipeline_versions:
            raise LoadError(f"pipeline {start.pipeline_id} has no version {start.version}")
        self._loader.executions[start.execution_id] = {
            "pipeline_id": start.pipeline_id,
            "version": start.version,
            "trigger": start.trigger,
            "status": "running",
            "started_at": start.started_at,
            "ended_at": None,
            "rows_in": None,
            "rows_out": None,
            "failed_step": None,
            "error_class": None,
            "error_message": None,
            "input_run_id": None,
        }
        self._loader.step_runs[start.execution_id] = {}

    def last_ingest(self, source: str, dataset: str) -> UUID | None:
        state = self._loader.states.get((source, dataset))
        return None if state is None else state.run_id

    def record_input(self, execution_id: UUID, ingest_run_id: UUID) -> None:
        execution = self._loader.executions.get(execution_id)
        if execution is None or execution["status"] != "running":
            raise LoadError(f"execution {execution_id} is not running")
        execution["input_run_id"] = ingest_run_id

    def record_step(self, execution_id: UUID, step: StepRun) -> None:
        if not 1 <= step.position <= len(self._owner(execution_id).steps):
            raise LoadError(f"execution {execution_id} ran no pipeline with a step {step.position}")
        steps = self._loader.step_runs[execution_id]
        known = steps.get(step.position)
        if known is not None and known.status != "running":
            raise LoadError(f"step {step.position} of execution {execution_id} has already ended")
        steps[step.position] = step

    def finish_execution(
        self,
        execution_id: UUID,
        *,
        ended_at: datetime,
        rows_in: int | None,
        rows_out: int | None,
        failure: RunFailure | None = None,
        failed_step: int | None = None,
    ) -> None:
        if failed_step is not None and failure is None:
            raise ValueError("only a failed execution names the step that failed")
        execution = self._loader.executions.get(execution_id)
        if execution is None or execution["status"] != "running":
            raise LoadError(f"execution {execution_id} is not running")
        owner = self._owner(execution_id)
        staging = _stage_name(Stage.STAGING, owner.source, owner.dataset)
        if failure is None and staging not in self._loader.staging:
            raise LoadError(f"execution {execution_id} has no STAGING table to publish as CLEAN")
        execution.update(
            status="succeeded" if failure is None else "failed",
            ended_at=ended_at,
            rows_in=rows_in,
            rows_out=rows_out,
            failed_step=failed_step,
            error_class=None if failure is None else failure.error_class,
            error_message=None if failure is None else failure.message,
        )
        frame = self._loader.staging.pop(staging, None)
        if failure is None and frame is not None:
            self._loader.clean[_stage_name(Stage.CLEAN, owner.source, owner.dataset)] = frame

    def read_execution(self, execution_id: UUID) -> Execution | None:
        execution = self._loader.executions.get(execution_id)
        if execution is None:
            return None
        owner = self._owner(execution_id)
        steps = self._loader.step_runs[execution_id]
        return Execution(
            execution_id=execution_id,
            source=owner.source,
            dataset=owner.dataset,
            steps=tuple(steps[position] for position in sorted(steps)),
            **execution,
        )

    def record_profile(self, profile: Profile) -> int:
        self._loader.stored_profiles.append(profile)
        return len(self._loader.stored_profiles)

    def read_profiles(
        self, source: str, dataset: str, stage: Stage | None = None, run: RunRef | None = None
    ) -> list[StoredProfile]:
        return [
            StoredProfile(profile_id, profile)
            for profile_id, profile in enumerate(self._loader.stored_profiles, 1)
            if (profile.source, profile.dataset) == (source, dataset)
            and (stage is None or profile.stage is Stage(stage))
            and (run is None or profile.run == run)
        ]

    def record_constraint_results(self, results: Sequence[ConstraintResult]) -> None:
        self._loader.constraint_results.extend(results)

    def read_constraint_results(
        self, source: str, dataset: str, stage: Stage | None = None, run: RunRef | None = None
    ) -> list[ConstraintResult]:
        return [
            result
            for result in self._loader.constraint_results
            if (result.source, result.dataset) == (source, dataset)
            and (stage is None or result.stage is Stage(stage))
            and (run is None or result.run == run)
        ]

    def record_lineage(
        self,
        source: str,
        dataset: str,
        run: RunRef,
        nodes: Sequence[LineageNode],
        recorded_at: datetime,
    ) -> None:
        recorded = self._loader.lineage.setdefault(run, [])
        check_chain(source, dataset, run, [*recorded, *nodes])
        recorded.extend(nodes)

    def read_lineage(self, run: RunRef) -> tuple[LineageNode, ...]:
        return tuple(self._loader.lineage.get(run, ()))


class MemoryLoader:
    """In-memory stand-in for PostgresLoader, held to the same contract."""

    def __init__(self) -> None:
        # V2: the stage tables a pipeline run writes, and its records.
        self.staging: dict[str, pl.DataFrame] = {}
        self.clean: dict[str, pl.DataFrame] = {}
        self.pipelines: dict[tuple[str, str, str], int] = {}
        self.pipeline_versions: dict[tuple[int, int], PipelineVersion] = {}
        self.executions: dict[UUID, dict[str, Any]] = {}
        self.step_runs: dict[UUID, dict[int, StepRun]] = {}
        self.stored_profiles: list[Profile] = []
        self.constraint_results: list[ConstraintResult] = []
        self.lineage: dict[RunRef, list[LineageNode]] = {}
        self.tables: dict[str, pl.DataFrame] = {}
        self.raw: dict[str, pl.DataFrame] = {}
        self.states: dict[tuple[str, str], DatasetState] = {}
        self.versions: dict[tuple[str, str], list[list[Column]]] = {}
        self.runs: dict[UUID, dict[str, Any]] = {}
        self.quarantine: list[dict[str, Any]] = []
        self.quality_results: list[dict[str, Any]] = []
        self.locks: set[tuple[str, str]] = set()
        self.sources: dict[str, dict[str, Any]] = {}
        self.datasets: dict[tuple[str, str], dict[str, Any]] = {}
        self.overrides: dict[str, dict[str, dict[str, Any]]] = {}

    def read_overrides(self, source: str) -> dict[str, dict[str, Any]]:
        return self.overrides.get(source, {})

    def record_config(self, copy: ConfigCopy) -> None:
        self.sources[copy.source] = {
            "source": copy.source,
            "connector_type": copy.connector_type,
            "connection": copy.connection,
            "run_id": copy.run_id,
            "recorded_at": copy.recorded_at,
        }
        self.datasets[(copy.source, copy.dataset)] = {
            "source": copy.source,
            "dataset": copy.dataset,
            "table_name": copy.table,
            "load_mode": copy.load_mode,
            "primary_key": list(copy.primary_key),
            "watermark": copy.watermark,
            "schedule": copy.schedule,
            "definition": copy.definition,
            "run_id": copy.run_id,
            "recorded_at": copy.recorded_at,
        }

    def lock_dataset(self, source: str, dataset: str) -> bool:
        if (source, dataset) in self.locks:
            return False
        self.locks.add((source, dataset))
        return True

    def unlock_dataset(self, source: str, dataset: str) -> None:
        self.locks.discard((source, dataset))

    def skip_run(self, run: RunStart, *, ended_at: datetime) -> None:
        self.start_run(run)
        self.runs[run.run_id].update(status="skipped", ended_at=ended_at)

    def fail_interrupted_runs(
        self, source: str, dataset: str, *, found_by: UUID, ended_at: datetime
    ) -> int:
        interrupted = [
            run
            for run in self.runs.values()
            if (run["source"], run["dataset"], run["status"]) == (source, dataset, "running")
        ]
        for run in interrupted:
            run.update(
                status="failed",
                ended_at=ended_at,
                error_class=INTERRUPTED,
                error_message=interrupted_message(found_by),
            )
        return len(interrupted)

    def start_run(self, run: RunStart) -> None:
        self.runs[run.run_id] = self._running_row(run)

    @staticmethod
    def _running_row(run: RunStart) -> dict[str, Any]:
        return {
            "run_id": run.run_id,
            "source": run.source,
            "dataset": run.dataset,
            "trigger": run.trigger,
            "status": "running",
            "started_at": run.started_at,
            "ended_at": None,
            "rows_extracted": None,
            "rows_loaded": None,
            "error_class": None,
            "error_message": None,
            "error_traceback": None,
            "rows_quarantined": None,
        }

    @contextmanager
    def stages(self) -> Iterator[MemoryStages]:
        yield MemoryStages(self)

    @contextmanager
    def transaction(self) -> Iterator[MemoryTransaction]:
        transaction = MemoryTransaction(self)
        yield transaction
        for name, frame in transaction.tables.items():
            if frame is _DROPPED:
                self.tables.pop(name, None)
            else:
                self.tables[name] = frame
        for name, frame in transaction.raw.items():
            if frame is _DROPPED:
                self.raw.pop(name, None)
            else:
                self.raw[name] = frame
        self.states.update(transaction.states)
        for key, added in transaction.versions.items():
            self.versions.setdefault(key, []).extend(added)
        for run_id, fields in transaction.run_updates.items():
            self.runs[run_id].update(fields)
        self.quarantine.extend(transaction.quarantine)
        self.quality_results.extend(transaction.quality_results)
        for run, nodes in transaction.lineage.items():
            self.lineage.setdefault(run, []).extend(nodes)

    def fail_run(
        self,
        start: RunStart,
        *,
        ended_at: datetime,
        rows_extracted: int | None,
        failure: RunFailure,
    ) -> None:
        run = self.runs.get(start.run_id)
        if run is None:
            # A run that failed before start_run still gets its row: Postgres writes one in the
            # same statement, so this must not go back through start_run.
            run = self.runs.setdefault(start.run_id, self._running_row(start))
        elif run["status"] != "running":
            raise LoadError(f"run {start.run_id} has already ended")
        run.update(
            status="failed",
            ended_at=ended_at,
            rows_extracted=rows_extracted,
            error_class=failure.error_class,
            error_message=failure.message,
            error_traceback=failure.traceback,
        )
