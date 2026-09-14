import traceback
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid7

import structlog

from udp.config.source import SourceConfig
from udp.connectors import CONNECTORS
from udp.connectors.base import ExtractRequest
from udp.names import table_name
from udp.pipeline.extract import RowCounter, extract
from udp.pipeline.load import load
from udp.pipeline.transform import transform
from udp.pipeline.validate import validate
from udp.storage.loader import Loader, RunFailure, RunStart

log = structlog.get_logger(step="run")

CHUNK_SIZE = 100_000


@dataclass(frozen=True)
class RunOutcome:
    run_id: UUID
    dataset: str
    status: Literal["succeeded", "failed"]
    rows_loaded: int | None


def run_source(
    source: str,
    config: SourceConfig[Any, Any],
    sources_dir: Path,
    loader: Loader,
    *,
    trigger: Literal["manual", "scheduled"] = "manual",
    chunk_size: int = CHUNK_SIZE,
) -> list[RunOutcome]:
    """Run every dataset of an already-validated source, one run each.

    This is the only place pipeline errors are caught: a failed dataset is recorded
    and logged, and the next dataset still runs.
    """
    connector = CONNECTORS[config.connection.type]
    outcomes = []
    for dataset in config.datasets:
        run_id = uuid7()
        counter = RowCounter()
        with structlog.contextvars.bound_contextvars(
            run_id=str(run_id), source=source, dataset=dataset.name
        ):
            try:
                started_at = datetime.now(UTC)
                loader.start_run(RunStart(run_id, source, dataset.name, trigger, started_at))
                log.info("run started", trigger=trigger)
                request = ExtractRequest(
                    sources_dir / source, config.connection, dataset, chunk_size
                )
                chunks = transform(validate(extract(connector, request, counter)))
                with loader.transaction() as transaction:
                    rows = load(
                        transaction, table_name(source, dataset.name), chunks, run_id, started_at
                    )
                    transaction.succeed_run(
                        run_id,
                        ended_at=datetime.now(UTC),
                        rows_extracted=counter.rows,
                        rows_loaded=rows,
                    )
            except Exception as error:
                outcomes.append(_record_failure(loader, run_id, dataset.name, counter, error))
                continue
            log.info(
                "run finished", status="succeeded", rows_extracted=counter.rows, rows_loaded=rows
            )
            outcomes.append(RunOutcome(run_id, dataset.name, "succeeded", rows))
    return outcomes


def _record_failure(
    loader: Loader, run_id: UUID, dataset: str, counter: RowCounter, error: Exception
) -> RunOutcome:
    failure = RunFailure(
        error_class=type(error).__name__,
        message=str(error),
        traceback="".join(traceback.format_exception(error)),
    )
    log.error(
        "run finished",
        status="failed",
        rows_extracted=counter.rows,
        error_class=failure.error_class,
        error=failure.message,
    )
    try:
        loader.fail_run(
            run_id, ended_at=datetime.now(UTC), rows_extracted=counter.rows, failure=failure
        )
    except Exception as recording_error:
        log.error("could not record the failed run", error=str(recording_error))
    return RunOutcome(run_id, dataset, "failed", None)
