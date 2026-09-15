import re
import threading
from collections.abc import Callable, Iterator
from contextlib import nullcontext
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid7

import polars as pl
import psycopg
import pytest
from apscheduler.schedulers.background import BackgroundScheduler
from fakes import MemoryLoader
from hypothesis import given
from hypothesis import strategies as st
from structlog.testing import capture_logs

from udp.config.schedule import check_schedule, cron_trigger
from udp.config.source import load_source
from udp.connectors import CONNECTORS
from udp.connectors.base import ExtractRequest
from udp.connectors.csv import CsvConnection, CsvConnector, CsvDataset
from udp.orchestration.scheduler import (
    RUNS_AT_ONCE,
    ScheduledDataset,
    add_jobs,
    find_schedules,
    run_scheduled,
)
from udp.pipeline.runner import RunOutcome, run_source
from udp.settings import Settings
from udp.storage.loader import INTERRUPTED, RunStart
from udp.storage.postgres import PostgresLoader

# --- the schedule check against a plain cron model --------------------------------------------

MODEL_MONTHS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
MODEL_WEEKDAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
MODEL_MONTH_DAYS = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
WINDOW = timedelta(days=9 * 365)


def _model_field(text: str, low: int, high: int, names: list[str]) -> set[int] | None:
    """The values a cron field selects, or None when the model refuses it."""

    def number(token: str) -> int | None:
        if token in names:
            return low + names.index(token)
        if re.fullmatch("[0-9]+", token) and low <= int(token) <= high:
            return int(token)
        return None

    selected: set[int] = set()
    for part in text.split(","):
        match = re.fullmatch(r"(\*|([a-z0-9]+)(?:-([a-z0-9]+))?)(?:/([0-9]+))?", part)
        if match is None:
            return None
        whole, first, last, step = match.groups()
        if whole == "*":
            start, end = low, high
        else:
            start_or_none = number(first)
            end_or_none = number(last) if last is not None else start_or_none
            if start_or_none is None or end_or_none is None or start_or_none > end_or_none:
                return None
            start, end = start_or_none, end_or_none
            numbers_only = first.isdigit() and (last or "0").isdigit()
            if step is not None and (last is None or not numbers_only):
                return None
        every = 1 if step is None else int(step)
        if every == 0 or (step is not None and every > end - start):
            return None
        selected.update(range(start, end + 1, every))
    return selected


def _model(expression: str) -> tuple[set[int], set[int], set[int], set[int], set[int]] | None:
    fields = expression.split()
    if len(fields) != 5:
        return None
    minute, hour, day, month, weekday = fields
    minutes = _model_field(minute, 0, 59, [])
    hours = _model_field(hour, 0, 23, [])
    days = _model_field(day, 1, 31, [])
    months = _model_field(month, 1, 12, MODEL_MONTHS)
    if weekday == "*":
        weekdays: set[int] | None = set(range(7))
    else:
        weekdays = set()
        for part in weekday.split(","):
            match = re.fullmatch("([a-z]+)(?:-([a-z]+))?", part)
            if match is None or {match[1], match[2] or match[1]} - set(MODEL_WEEKDAYS):
                return None
            start = MODEL_WEEKDAYS.index(match[1])
            end = MODEL_WEEKDAYS.index(match[2] or match[1])
            if start > end:
                return None
            weekdays.update(range(start, end + 1))
        if day != "*":
            return None
    if minutes is None or hours is None or days is None or months is None or weekdays is None:
        return None
    if not any(d <= MODEL_MONTH_DAYS[m - 1] for d in days for m in months):
        return None
    return minutes, hours, days, months, weekdays


def _model_fire_times(expression: str, start: datetime, count: int) -> list[datetime]:
    model = _model(expression)
    assert model is not None
    minutes, hours, days, months, weekdays = model
    found: list[datetime] = []
    day = start.date()
    while day <= (start + WINDOW).date() and len(found) < count:
        if day.day in days and day.month in months and day.weekday() in weekdays:
            for hour in sorted(hours):
                for minute in sorted(minutes):
                    moment = datetime(day.year, day.month, day.day, hour, minute, tzinfo=UTC)
                    if start <= moment <= start + WINDOW and len(found) < count:
                        found.append(moment)
        day += timedelta(days=1)
    return found


def _trigger_fire_times(expression: str, start: datetime, count: int) -> list[datetime]:
    trigger = cron_trigger(expression)
    found: list[datetime] = []
    now = start
    while len(found) < count:
        moment = trigger.get_next_fire_time(None, now)
        if moment is None or moment > start + WINDOW:
            break
        found.append(moment)
        now = moment + timedelta(microseconds=1)
    return found


@st.composite
def _numeric_part(draw: st.DrawFn, low: int, high: int) -> str:
    """A part whose values and steps sit on and just past the field's limits."""

    def near(value: int) -> int:
        return draw(st.sampled_from([value - 1, value, value + 1]))

    first = near(draw(st.sampled_from([low, (low + high) // 2, high])))
    last = draw(st.sampled_from([first - 1, first, first + 1, near(high)]))
    kind = draw(st.sampled_from(["*", "n", "a-b", "*/s", "a-b/s", "n/s"]))
    width = high - low if kind.startswith("*") else last - first
    step = draw(st.sampled_from([0, 1, width - 1, width, width + 1]).filter(lambda s: s >= 0))
    return (
        kind.replace("a", str(first))
        .replace("b", str(last))
        .replace("n", str(first))
        .replace("s", str(step))
    )


def _listed(part: st.SearchStrategy[str]) -> st.SearchStrategy[str]:
    return st.lists(part, min_size=1, max_size=3).map(",".join)


MONTH_NAME = st.sampled_from([*MODEL_MONTHS, "JAN", "foo"])
WEEKDAY_NAME = st.sampled_from([*MODEL_WEEKDAYS, "MON", "sunday"])
FIELDS = [
    (_listed(_numeric_part(0, 59)), ["*", "0", "*/15", "5-10"]),
    (_listed(_numeric_part(0, 23)), ["*", "6", "0-23/6"]),
    (
        st.one_of(_listed(_numeric_part(1, 31)), st.sampled_from(["29", "30", "31", "29,30"])),
        ["*", "1", "15"],
    ),
    (
        st.one_of(
            _listed(st.one_of(_numeric_part(1, 12), MONTH_NAME)),
            st.tuples(MONTH_NAME, MONTH_NAME).map("-".join),
            st.sampled_from(["2", "feb", "4", "4,6", "jan-mar/2", "jan-3", "2,4", "apr,jun"]),
        ),
        ["*", "jan", "1-6"],
    ),
    (
        st.one_of(
            _listed(st.one_of(WEEKDAY_NAME, st.tuples(WEEKDAY_NAME, WEEKDAY_NAME).map("-".join))),
            st.sampled_from(["sat-sun", "sun-mon", "1-5", "0", "*/2", "mon/2", "mon,,tue"]),
        ),
        ["*", "*", "mon-fri"],
    ),
]


@st.composite
def _expression(draw: st.DrawFn) -> str:
    """One field drawn to break the rules, the others plain, sometimes too few or many."""
    focus = draw(st.integers(0, 4))
    fields = [
        draw(near_miss if index == focus else st.sampled_from(plain))
        for index, (near_miss, plain) in enumerate(FIELDS)
    ]
    count = draw(st.sampled_from([5, 5, 5, 5, 4, 6]))
    return " ".join([*fields, "*"][:count])


EXPRESSION = _expression()
START = st.builds(
    lambda day, minute, second: (
        datetime.combine(day, datetime.min.time(), UTC) + timedelta(minutes=minute, seconds=second)
    ),
    st.dates(date(2020, 1, 1), date(2035, 12, 31)),
    st.integers(0, 24 * 60 - 1),
    st.sampled_from([0, 0, 0, 1, 59]),
)


@given(EXPRESSION, START)
def test_a_schedule_is_accepted_exactly_when_cron_reads_it_and_fires_when_cron_would(
    expression: str, start: datetime
) -> None:
    try:
        check_schedule(expression)
        accepted = True
    except ValueError:
        accepted = False

    assert accepted == (_model(expression) is not None)
    if accepted:
        assert _trigger_fire_times(expression, start, 3) == _model_fire_times(expression, start, 3)


# --- runs that overlap or were interrupted ----------------------------------------------------

ROWS = "id,name\n1,Anna\n2,Bram\n"


def _sources(root: Path, scheduled: frozenset[str] = frozenset()) -> Path:
    """A CSV source `shop` with datasets `a` and `b`; scheduled ones run every five minutes."""
    sources = root / "sources"
    folder = sources / "shop"
    folder.mkdir(parents=True)
    text = "connection:\n  type: csv\ndatasets:\n"
    for name in ("a", "b"):
        text += f"  - name: {name}\n    path: {name}.csv\n"
        if name in scheduled:
            text += "    schedule: '*/5 * * * *'\n"
        (folder / f"{name}.csv").write_text(ROWS, encoding="utf-8")
    (folder / "source.yaml").write_text(text, encoding="utf-8")
    return sources


def _only(sources: Path, dataset: str) -> Any:
    config = load_source(sources, "shop", {})
    return config.model_copy(update={"datasets": [d for d in config.datasets if d.name == dataset]})


class ConnectorStartingARun(CsvConnector):
    """Starts another run from inside the first extract, as a run in another process would."""

    def __init__(self, start: Callable[[], list[RunOutcome]]) -> None:
        self.start = start
        self.outcomes: list[RunOutcome] | None = None

    def extract(self, request: ExtractRequest[CsvConnection, CsvDataset]) -> Iterator[pl.DataFrame]:
        if self.outcomes is None:
            self.outcomes = []
            self.outcomes.extend(self.start())
        yield from super().extract(request)


@pytest.mark.parametrize(("nested", "status"), [("a", "skipped"), ("b", "succeeded")])
def test_a_run_started_while_its_dataset_is_loading_is_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, nested: str, status: str
) -> None:
    sources = _sources(tmp_path)
    loader = MemoryLoader()
    connector = ConnectorStartingARun(
        lambda: run_source("shop", _only(sources, nested), sources, loader)
    )
    monkeypatch.setitem(CONNECTORS, "csv", connector)

    (outer,) = run_source("shop", _only(sources, "a"), sources, loader)

    assert connector.outcomes is not None
    (inner,) = connector.outcomes
    assert (outer.status, inner.status) == ("succeeded", status)
    record = loader.runs[inner.run_id]
    assert (record["dataset"], record["status"]) == (nested, status)
    assert record["ended_at"] is not None
    assert loader.runs[outer.run_id]["status"] == "succeeded"
    assert loader.locks == set()


def test_a_dead_run_is_marked_interrupted_when_its_dataset_runs_again(tmp_path: Path) -> None:
    sources = _sources(tmp_path)
    loader = MemoryLoader()
    dead_a = RunStart(uuid7(), "shop", "a", "scheduled", datetime.now(UTC))
    dead_b = RunStart(uuid7(), "shop", "b", "scheduled", datetime.now(UTC))
    loader.start_run(dead_a)
    loader.start_run(dead_b)

    with capture_logs() as logs:
        (outcome,) = run_source("shop", _only(sources, "a"), sources, loader)

    assert outcome.status == "succeeded"
    found = loader.runs[dead_a.run_id]
    assert (found["status"], found["error_class"]) == ("failed", INTERRUPTED)
    assert str(outcome.run_id) in found["error_message"]
    assert loader.runs[dead_b.run_id]["status"] == "running"
    assert [
        line["count"] for line in logs if line["event"] == "interrupted runs marked failed"
    ] == [1]


@pytest.mark.db
def test_a_run_on_another_postgres_connection_is_skipped_while_the_dataset_loads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    url = Settings().database_url  # type: ignore[call-arg]
    sources = _sources(tmp_path)
    with PostgresLoader(url) as first, PostgresLoader(url) as second:
        connector = ConnectorStartingARun(
            lambda: run_source("shop", _only(sources, "a"), sources, second)
        )
        monkeypatch.setitem(CONNECTORS, "csv", connector)

        (outer,) = run_source("shop", _only(sources, "a"), sources, first)

    assert connector.outcomes is not None
    (inner,) = connector.outcomes
    assert (outer.status, inner.status) == ("succeeded", "skipped")
    with psycopg.connect(url) as conn:
        row = conn.execute(
            "SELECT status, ended_at IS NOT NULL FROM platform.pipeline_runs WHERE run_id = %s",
            [inner.run_id],
        ).fetchone()
    assert row == ("skipped", True)


def test_a_lock_that_cannot_be_taken_fails_the_run_instead_of_raising(tmp_path: Path) -> None:
    sources = _sources(tmp_path)

    class LostConnection(MemoryLoader):
        def lock_dataset(self, source: str, dataset: str) -> bool:
            raise OSError("connection lost")

    (outcome,) = run_source("shop", _only(sources, "a"), sources, LostConnection())

    assert outcome.status == "failed"


# --- the scheduler ------------------------------------------------------------------------------

DEMO_ENV = {
    "DEMO_DB_URL": "sqlite:///demo.db",
    "DEMO_API_URL": "http://api.test",
    "DEMO_API_TOKEN": "t",
}


def test_demo_csv_is_the_scheduled_demo_dataset() -> None:
    assert find_schedules(Path("sources"), DEMO_ENV) == [
        ScheduledDataset("demo_csv", "customers", "* * * * *")
    ]


def test_an_invalid_source_is_logged_and_the_others_are_still_scheduled(tmp_path: Path) -> None:
    sources = _sources(tmp_path, frozenset({"a"}))
    (sources / "broken").mkdir()
    (sources / "broken" / "source.yaml").write_text("connection:\n  type: nope\n", encoding="utf-8")

    with capture_logs() as logs:
        found = find_schedules(sources, {})

    assert found == [ScheduledDataset("shop", "a", "*/5 * * * *")]
    (error,) = [line for line in logs if line["log_level"] == "error"]
    assert error["source"] == "broken"
    assert "sources/broken/source.yaml" in error["error"]


def test_each_scheduled_dataset_becomes_a_utc_cron_job() -> None:
    scheduler = BackgroundScheduler(timezone=UTC)
    now = datetime(2026, 9, 15, 6, 7, 30, tzinfo=UTC)

    with capture_logs() as logs:
        add_jobs(scheduler, [ScheduledDataset("shop", "a", "*/5 * * * *")], print, now=now)

    (job,) = scheduler.get_jobs()
    assert (job.id, job.args, job.coalesce, job.misfire_grace_time, job.max_instances) == (
        "shop.a",
        ("shop", "a"),
        True,
        None,
        RUNS_AT_ONCE,
    )
    assert job.trigger.timezone == UTC
    (line,) = logs
    assert line["next_run"] == "2026-09-15T06:10:00+00:00"


def test_a_firing_runs_only_its_dataset_as_a_scheduled_run(tmp_path: Path) -> None:
    sources = _sources(tmp_path, frozenset({"a"}))
    loader = MemoryLoader()
    fired = threading.Event()

    def run(source: str, dataset: str) -> None:
        run_scheduled(sources, source, dataset, {}, lambda: nullcontext(loader))
        fired.set()

    scheduler = BackgroundScheduler(timezone=UTC)
    add_jobs(scheduler, find_schedules(sources, {}), run)
    scheduler.start()
    try:
        scheduler.modify_job("shop.a", next_run_time=datetime.now(UTC))
        assert fired.wait(5)
    finally:
        scheduler.shutdown()

    (record,) = loader.runs.values()
    assert (record["dataset"], record["trigger"], record["status"]) == (
        "a",
        "scheduled",
        "succeeded",
    )


@pytest.mark.parametrize(
    ("dataset", "config", "event"),
    [
        ("gone", "", "scheduled dataset is no longer in its source config; run not started"),
        ("a", "connection:\n  type: nope\n", "invalid source config; scheduled run not started"),
    ],
)
def test_a_firing_that_cannot_run_logs_why_and_records_nothing(
    tmp_path: Path, dataset: str, config: str, event: str
) -> None:
    sources = _sources(tmp_path)
    if config:
        (sources / "shop" / "source.yaml").write_text(config, encoding="utf-8")
    loader = MemoryLoader()

    with capture_logs() as logs:
        assert run_scheduled(sources, "shop", dataset, {}, lambda: nullcontext(loader)) == []

    assert [line["event"] for line in logs] == [event]
    assert loader.runs == {}
