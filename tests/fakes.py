from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Any
from uuid import UUID

import polars as pl

from udp.errors import LoadError
from udp.storage.loader import RunFailure, RunStart, columns_mismatch, table_columns


class MemoryTransaction:
    def __init__(self, loader: MemoryLoader) -> None:
        self._loader = loader
        self.tables: dict[str, pl.DataFrame] = {}
        self.run_updates: dict[UUID, dict[str, Any]] = {}

    def replace_table(self, table: str, chunks: Iterable[pl.DataFrame]) -> int:
        frames = list(chunks)
        if not frames:
            raise LoadError(f"no data to load into datasets.{table}")
        frame = pl.concat(frames)
        columns = table_columns(frame)
        existing = self.tables.get(table, self._loader.tables.get(table))
        if existing is not None and table_columns(existing) != columns:
            raise columns_mismatch(table, table_columns(existing), columns)
        self.tables[table] = frame
        return frame.height

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
        self.tables.update(transaction.tables)
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
