"""Running datasets on their `schedule:`.

Schedules are read once when the scheduler starts. Each firing reads its source.yaml again and
runs only its dataset through the same runner as `udp run`, on its own database connection, so
an overlap with any other run of that dataset is recorded by the runner as skipped.
"""

import logging
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import structlog
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MAX_INSTANCES, EVENT_JOB_MISSED
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.blocking import BlockingScheduler

from udp.config.schedule import cron_trigger
from udp.config.source import load_source
from udp.errors import ConfigError
from udp.pipeline.runner import RunOutcome, run_source
from udp.storage.loader import Loader
from udp.storage.postgres import PostgresLoader

log = structlog.get_logger(step="schedule")

RUNS_AT_ONCE = 4


@dataclass(frozen=True)
class ScheduledDataset:
    source: str
    dataset: str
    schedule: str


def find_schedules(
    sources_dir: Path, env: Mapping[str, str], loader: Loader | None
) -> list[ScheduledDataset]:
    """Every dataset with a schedule; an invalid source is logged and left out.

    A schedule edited from the dashboard is stored in the platform database, so each source's
    edits are read from the loader. `None` means the files alone, and is spelled out at every
    call so that reading no edits is never what a forgotten argument does.
    """
    found: list[ScheduledDataset] = []
    for path in sorted(sources_dir.glob("*/source.yaml")):
        source = path.parent.name
        try:
            overrides = loader.read_overrides(source) if loader is not None else {}
            config = load_source(sources_dir, source, env, overrides)
        except ConfigError as error:
            log.error(
                "invalid source config; its datasets are not scheduled",
                source=source,
                error=str(error),
            )
            continue
        found.extend(
            ScheduledDataset(source, dataset.name, dataset.schedule)
            for dataset in config.datasets
            if dataset.schedule is not None
        )
    return found


def run_scheduled(
    sources_dir: Path,
    source: str,
    dataset: str,
    env: Mapping[str, str],
    open_loader: Callable[[], AbstractContextManager[Loader]],
) -> list[RunOutcome]:
    """Run one dataset as a scheduled run, reading its source.yaml and its edits as they are now."""
    with open_loader() as loader:
        try:
            config = load_source(sources_dir, source, env, loader.read_overrides(source))
        except ConfigError as error:
            log.error(
                "invalid source config; scheduled run not started",
                source=source,
                dataset=dataset,
                error=str(error),
            )
            return []
        chosen = [item for item in config.datasets if item.name == dataset]
        if not chosen:
            log.error(
                "scheduled dataset is no longer in its source config; run not started",
                source=source,
                dataset=dataset,
            )
            return []
        return run_source(
            source,
            config.model_copy(update={"datasets": chosen}),
            sources_dir,
            loader,
            trigger="scheduled",
        )


def add_jobs(
    scheduler: Any,
    schedules: list[ScheduledDataset],
    run: Callable[[str, str], object],
    now: datetime | None = None,
) -> None:
    """One job per scheduled dataset. A firing missed while the scheduler was busy or down
    runs once. APScheduler may start a job again while an earlier copy still runs, so the
    runner's lock, not APScheduler, decides and records an overlap; the executor's pool of
    RUNS_AT_ONCE threads limits runs across all datasets together."""
    moment = now or datetime.now(UTC)
    for item in schedules:
        trigger = cron_trigger(item.schedule)
        scheduler.add_job(
            run,
            trigger=trigger,
            args=[item.source, item.dataset],
            id=f"{item.source}.{item.dataset}",
            coalesce=True,
            misfire_grace_time=None,
            max_instances=RUNS_AT_ONCE,
            replace_existing=True,
        )
        next_run = trigger.get_next_fire_time(None, moment)
        log.info(
            "dataset scheduled",
            source=item.source,
            dataset=item.dataset,
            schedule=item.schedule,
            next_run=next_run.isoformat(),
        )


def _log_event(event: Any) -> None:
    if event.code == EVENT_JOB_ERROR:
        log.error("scheduled job failed", job=event.job_id, error=repr(event.exception))
    elif event.code == EVENT_JOB_MISSED:
        log.warning(
            "scheduled run missed",
            job=event.job_id,
            scheduled_for=event.scheduled_run_time.isoformat(),
        )
    else:
        log.warning("scheduled run not started; too many copies running", job=event.job_id)


def serve(sources_dir: Path, database_url: str, env: Mapping[str, str]) -> None:
    """Run scheduled datasets until the process is stopped."""
    apscheduler_log = logging.getLogger("apscheduler")
    apscheduler_log.addHandler(logging.NullHandler())
    apscheduler_log.propagate = False
    scheduler = BlockingScheduler(
        timezone=UTC, executors={"default": ThreadPoolExecutor(RUNS_AT_ONCE)}
    )
    scheduler.add_listener(_log_event, EVENT_JOB_ERROR | EVENT_JOB_MISSED | EVENT_JOB_MAX_INSTANCES)

    def run(source: str, dataset: str) -> None:
        run_scheduled(sources_dir, source, dataset, env, lambda: PostgresLoader(database_url))

    with PostgresLoader(database_url) as loader:
        schedules = find_schedules(sources_dir, env, loader)
    if not schedules:
        log.warning("no dataset has a schedule")
    add_jobs(scheduler, schedules, run)
    log.info("scheduler started", datasets=len(schedules), runs_at_once=RUNS_AT_ONCE)
    try:
        scheduler.start()
    except KeyboardInterrupt:
        scheduler.shutdown(wait=False)
    log.info("scheduler stopped")
