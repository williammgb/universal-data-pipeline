from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime
from typing import Any
from uuid import UUID

import polars as pl

from udp.errors import LoadError
from udp.storage.loader import (
    Column,
    ColumnChanges,
    DatasetState,
    LoadResult,
    RunFailure,
    RunStart,
    column_changes,
    table_columns,
)

_DROPPED = object()


class MemoryTransaction:
    def __init__(self, loader: MemoryLoader) -> None:
        self._loader = loader
        self.tables: dict[str, Any] = {}
        self.states: dict[tuple[str, str], DatasetState] = {}
        self.versions: dict[tuple[str, str], list[list[Column]]] = {}
        self.run_updates: dict[UUID, dict[str, Any]] = {}

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

    def succeed_run(
        self, run_id: UUID, *, ended_at: datetime, rows_extracted: int, rows_loaded: int
    ) -> None:
        run = self._loader.runs.get(run_id)
        if run is None or run["status"] != "running":
            raise LoadError(f"run {run_id} is not a running run")
        self.run_updates[run_id] = {
            "status": "succeeded",
            "ended_at": ended_at,
            "rows_extracted": rows_extracted,
            "rows_loaded": rows_loaded,
        }


class MemoryLoader:
    """In-memory stand-in for PostgresLoader, held to the same contract."""

    def __init__(self) -> None:
        self.tables: dict[str, pl.DataFrame] = {}
        self.states: dict[tuple[str, str], DatasetState] = {}
        self.versions: dict[tuple[str, str], list[list[Column]]] = {}
        self.runs: dict[UUID, dict[str, Any]] = {}

    def start_run(self, run: RunStart) -> None:
        self.runs[run.run_id] = {
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
        }

    @contextmanager
    def transaction(self) -> Iterator[MemoryTransaction]:
        transaction = MemoryTransaction(self)
        yield transaction
        for name, frame in transaction.tables.items():
            if frame is _DROPPED:
                self.tables.pop(name, None)
            else:
                self.tables[name] = frame
        self.states.update(transaction.states)
        for key, added in transaction.versions.items():
            self.versions.setdefault(key, []).extend(added)
        for run_id, fields in transaction.run_updates.items():
            self.runs[run_id].update(fields)

    def fail_run(
        self,
        run_id: UUID,
        *,
        ended_at: datetime,
        rows_extracted: int | None,
        failure: RunFailure,
    ) -> None:
        run = self.runs.get(run_id)
        if run is None or run["status"] != "running":
            raise LoadError(f"run {run_id} is not a running run")
        run.update(
            status="failed",
            ended_at=ended_at,
            rows_extracted=rows_extracted,
            error_class=failure.error_class,
            error_message=failure.message,
            error_traceback=failure.traceback,
        )
