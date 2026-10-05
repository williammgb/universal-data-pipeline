import traceback
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid7

import structlog

from udp.config.source import SourceConfig
from udp.connectors import CONNECTORS
from udp.connectors.base import Connector, ExtractRequest, SavedWatermark
from udp.errors import QualityError
from udp.names import table_name
from udp.pipeline.column_types import apply_column_types
from udp.pipeline.custom import TransformContext, apply_transform, find_transform, load_transform
from udp.pipeline.extract import RowCounter, extract
from udp.pipeline.incremental import (
    WatermarkTracker,
    check_same_load_settings,
    config_sha256,
    is_unchanged_file,
    new_rows,
)
from udp.pipeline.load import load
from udp.pipeline.transform import transform
from udp.pipeline.validate import validate
from udp.quality.checks import check_dataset, check_rows
from udp.quality.quarantine import threshold_exceeded
from udp.storage.loader import (
    ConfigCopy,
    DatasetState,
    Loader,
    LoadTransaction,
    RunFailure,
    RunFindings,
    RunStart,
)

log = structlog.get_logger(step="run")

CHUNK_SIZE = 100_000


@dataclass(frozen=True)
class RunOutcome:
    run_id: UUID
    dataset: str
    status: Literal["succeeded", "failed", "skipped"]
    rows_loaded: int | None


def run_source(
    source: str,
    config: SourceConfig[Any, Any],
    sources_dir: Path,
    loader: Loader,
    *,
    trigger: Literal["manual", "scheduled"] = "manual",
    chunk_size: int = CHUNK_SIZE,
    full_refresh: bool = False,
) -> list[RunOutcome]:
    """Run every dataset of an already-validated source, one run each.

    This is the only place pipeline errors are caught: a failed dataset is recorded
    and logged, and the next dataset still runs. A dataset another run is loading is
    recorded as skipped; its lock is held until its run reaches its final status.
    """
    connector = CONNECTORS[config.connection.type]
    outcomes = []
    for dataset in config.datasets:
        run_id = uuid7()
        counter = RowCounter()
        findings = RunFindings(source, dataset.name)
        locked = False
        with structlog.contextvars.bound_contextvars(
            run_id=str(run_id), source=source, dataset=dataset.name
        ):
            started_at = datetime.now(UTC)
            run = RunStart(run_id, source, dataset.name, trigger, started_at)
            try:
                if not loader.lock_dataset(source, dataset.name):
                    loader.skip_run(run, ended_at=datetime.now(UTC))
                    log.warning(
                        "run finished",
                        status="skipped",
                        trigger=trigger,
                        reason="another run of this dataset is in progress",
                    )
                    outcomes.append(RunOutcome(run_id, dataset.name, "skipped", None))
                    continue
                locked = True
                # Holding the lock means no other run is alive, so a `running` row is a dead run.
                interrupted = loader.fail_interrupted_runs(
                    source, dataset.name, found_by=run_id, ended_at=started_at
                )
                if interrupted:
                    log.warning("interrupted runs marked failed", count=interrupted)
                loader.start_run(run)
                loader.record_config(
                    ConfigCopy(
                        source=source,
                        connector_type=config.connection.type,
                        connection=config.written.connection,
                        dataset=dataset.name,
                        table=table_name(source, dataset.name),
                        load_mode=dataset.load_mode,
                        primary_key=tuple(dataset.primary_key or ()),
                        watermark=dataset.watermark,
                        schedule=dataset.schedule,
                        definition=config.written.datasets[dataset.name],
                        run_id=run_id,
                        recorded_at=started_at,
                    )
                )
                log.info("run started", trigger=trigger, full_refresh=full_refresh)
                request = ExtractRequest(
                    sources_dir / source, config.connection, dataset, chunk_size
                )
                with loader.transaction() as transaction:
                    rows = _load_dataset(
                        transaction,
                        connector,
                        source,
                        request,
                        counter,
                        run_id,
                        started_at,
                        full_refresh,
                        findings,
                    )
                    ended_at = datetime.now(UTC)
                    transaction.record_findings(run_id, findings, ended_at)
                    transaction.succeed_run(
                        run_id,
                        ended_at=ended_at,
                        rows_extracted=counter.rows,
                        rows_loaded=rows,
                    )
            except Exception as error:
                outcomes.append(_record_failure(loader, run, counter, error, findings))
                continue
            finally:
                if locked:
                    loader.unlock_dataset(source, dataset.name)
            log.info(
                "run finished", status="succeeded", rows_extracted=counter.rows, rows_loaded=rows
            )
            outcomes.append(RunOutcome(run_id, dataset.name, "succeeded", rows))
    return outcomes


def _load_dataset(
    transaction: LoadTransaction,
    connector: Connector[Any, Any],
    source: str,
    request: ExtractRequest[Any, Any],
    counter: RowCounter,
    run_id: UUID,
    started_at: datetime,
    full_refresh: bool,
    findings: RunFindings,
) -> int:
    """Load one dataset inside the caller's transaction; returns rows loaded."""
    dataset = request.dataset
    table = table_name(source, dataset.name)
    state = None if full_refresh else transaction.read_state(source, dataset.name)
    file = connector.file_version(request)
    transform_file = find_transform(request.source_dir)
    digest = config_sha256(dataset, transform_file.sha256 if transform_file else None)

    if is_unchanged_file(state, file, digest):
        log.info("file unchanged, skipped", step="extract", path=file.path if file else None)
        if dataset.checks or dataset.columns:
            check_dataset(transaction, table, dataset, findings, started_at)
        return 0
    if state is None:
        # A first load or a full refresh: RAW starts over with the table, so a column that
        # changed type can be loaded again.
        transaction.drop_table(table)
        transaction.drop_raw(source, dataset.name)
    else:
        check_same_load_settings(state, dataset)

    saved = state.watermark if state is not None else None
    incremental = dataset.load_mode != "full" and dataset.watermark is not None
    inclusive = dataset.load_mode == "merge"
    read_from = saved
    if incremental and state is not None and state.config_sha256 != digest:
        if inclusive:
            # Every row is read once more; the merge rewrites only rows whose result changed.
            read_from = None
            log.info("settings or transform.py changed; reading every source row once")
        else:
            log.info(
                "settings or transform.py changed; rows already loaded keep their earlier "
                "result until --full-refresh"
            )
    if incremental and read_from is not None:
        request = replace(
            request, watermark=SavedWatermark(dataset.watermark, read_from, inclusive)
        )
    tracker = WatermarkTracker()
    chunks = transform(validate(extract(connector, request, counter)))
    if dataset.columns:
        chunks = apply_column_types(chunks, dataset.columns, findings)
    if transform_file is not None:
        context = TransformContext(source, dataset.name, run_id)
        chunks = apply_transform(chunks, load_transform(transform_file), context, transform_file)
    if dataset.checks:
        chunks = check_rows(chunks, dataset.checks, findings)
    if incremental and dataset.watermark is not None:
        chunks = new_rows(
            chunks,
            watermark=dataset.watermark,
            primary_key=dataset.primary_key or (),
            saved=read_from,
            inclusive=inclusive,
            expected_kind=state.watermark_type if state is not None else None,
            tracker=tracker,
        )

    result = load(transaction, table, dataset, chunks, run_id, started_at)
    raw_rows = transaction.append_raw(source, dataset.name)
    log.info("added to RAW", step="load", rows=raw_rows)
    limit = dataset.quarantine_threshold_percent
    if threshold_exceeded(findings.quarantined_rows, counter.rows, limit):
        # transform.py can add rows, so rows can be quarantined when none were extracted.
        share = f" ({findings.quarantined_rows * 100 / counter.rows:.2f}%)" if counter.rows else ""
        raise QualityError(
            f"{findings.quarantined_rows} of {counter.rows} rows{share} quarantined, "
            f"above the dataset's limit of {limit:g}%"
        )
    if dataset.checks or dataset.columns:
        check_dataset(transaction, table, dataset, findings, started_at)
    if incremental and tracker.kind is None:
        # No rows arrived, so the column types are unknown: nothing is saved, and the next
        # run with data builds the table again as a first load.
        log.info("no rows to load; state not saved", step="load")
        return result.rows
    version = transaction.record_columns(table, source, dataset.name, run_id, started_at)
    if version is not None:
        log.info("schema version recorded", step="load", version=version)

    highest = tracker.highest
    if saved is not None and (highest is None or saved > highest):  # type: ignore[operator]
        highest = saved
    transaction.save_state(
        DatasetState(
            source=source,
            dataset=dataset.name,
            load_mode=dataset.load_mode,
            primary_key=tuple(dataset.primary_key or ()),
            watermark_column=dataset.watermark,
            watermark_type=tracker.kind,
            watermark=highest,
            file_path=file.path if file else None,
            file_sha256=file.sha256 if file else None,
            config_sha256=digest,
            run_id=run_id,
            saved_at=datetime.now(UTC),
        )
    )
    return result.rows


def _record_failure(
    loader: Loader,
    run: RunStart,
    counter: RowCounter,
    error: Exception,
    findings: RunFindings,
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
    ended_at = datetime.now(UTC)
    try:
        # The load was rolled back; what was quarantined and checked still explains the failure.
        with loader.transaction() as transaction:
            transaction.record_findings(run.run_id, findings, ended_at)
    except Exception as recording_error:
        log.error("could not record the failed run's findings", error=str(recording_error))
    try:
        loader.fail_run(run, ended_at=ended_at, rows_extracted=counter.rows, failure=failure)
    except Exception as recording_error:
        log.error("could not record the failed run", error=str(recording_error))
    return RunOutcome(run.run_id, run.dataset, "failed", None)
