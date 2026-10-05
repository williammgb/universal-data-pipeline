"""The execution engine: a pipeline run from RAW through its steps to CLEAN, and its record.

Every test runs twice: against the in-memory store, in the fast gate, and against a real
PostgreSQL, in the full gate (the `postgres` parameter is marked `db`).
"""

import re
import uuid
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

import polars as pl
import psycopg
import pytest
from fakes import MemoryLoader, raw_table
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from psycopg import sql
from pydantic import TypeAdapter
from typer.testing import CliRunner

from udp.cli import app
from udp.config.constraints import Constraint
from udp.config.pipeline import PipelineDefinition, ProfileChoice
from udp.connectors.base import DatasetBase
from udp.names import Stage, stage_table
from udp.pipeline.execution import (
    PipelineBusy,
    PipelineRun,
    carry_out,
    read_run,
    run_pipeline,
    start_pipeline,
)
from udp.pipeline.load import with_platform_columns
from udp.settings import Settings
from udp.storage.loader import INTERRUPTED, Loader, RunStart
from udp.storage.postgres import PostgresLoader
from udp.transformations.registry import parse_step

NOW = datetime(2026, 10, 5, 9, tzinfo=UTC)
ORDERS = pl.DataFrame(
    {
        "id": [1, 2, 3, 4],
        "name": ["  ada ", "bo", "cy", None],
        "email": ["a@x.nl", None, "c@x.nl", "d@x.nl"],
    }
)
TIDY: list[dict[str, Any]] = [
    {"type": "normalize_values", "columns": ["name"], "trim": True},
    {"type": "drop_missing", "columns": ["email"]},
]
ID_REQUIRED: dict[str, Any] = {"constraint": "not_null", "column": "id", "critical": True}


class Shop:
    """One throwaway dataset in one store: ingests into RAW, pipelines over it, its stage tables."""

    def __init__(self, loader: Loader, reader: psycopg.Connection | None) -> None:
        self.loader = loader
        self.reader = reader
        self.source = f"pex{uuid.uuid4().hex[:10]}"
        self.dataset = "orders"
        self.clock = iter(NOW + timedelta(seconds=second) for second in range(1_000_000))

    def now(self) -> datetime:
        return next(self.clock)

    def ingest(self, frame: pl.DataFrame = ORDERS) -> UUID:
        """An ingest run that appends the frame to RAW, recorded as succeeded."""
        run = RunStart(uuid.uuid7(), self.source, self.dataset, "manual", self.now())
        self.loader.start_run(run)
        rows = with_platform_columns(frame, run.run_id, run.started_at)
        if isinstance(self.loader, MemoryLoader):
            name = raw_table(self.source, self.dataset)
            existing = self.loader.raw.get(name)
            self.loader.raw[name] = (
                rows if existing is None else pl.concat([existing, rows], how="diagonal_relaxed")
            )
            self.loader.runs[run.run_id].update(status="succeeded", ended_at=self.now())
            return run.run_id
        with self.loader.stages() as stages:
            stages.append_raw(self.source, self.dataset, [rows])  # type: ignore[attr-defined]
        with self.loader.transaction() as transaction:
            transaction.succeed_run(
                run.run_id,
                ended_at=self.now(),
                rows_extracted=frame.height,
                rows_loaded=frame.height,
            )
        return run.run_id

    def pipeline(
        self,
        steps: Sequence[dict[str, Any]] = TIDY,
        *,
        name: str = "tidy",
        constraints: Sequence[dict[str, Any]] = (ID_REQUIRED,),
        profile: ProfileChoice = "ends",
        load_mode: Literal["full", "append", "merge"] = "append",
        primary_key: list[str] | None = None,
    ) -> PipelineDefinition:
        return PipelineDefinition(
            name=name,
            source=self.source,
            dataset=DatasetBase(
                name=self.dataset,
                load_mode=load_mode,
                watermark=None if load_mode == "full" else "id",
                primary_key=primary_key,
            ),
            profile=profile,
            constraints=tuple(TypeAdapter(Constraint).validate_python(c) for c in constraints),
            steps=tuple(parse_step(n, step) for n, step in enumerate(steps, 1)),
        )

    def run(self, pipeline: PipelineDefinition) -> PipelineRun:
        return run_pipeline(self.loader, pipeline, clock=self.now)

    def table(self, stage: Stage) -> pl.DataFrame | None:
        """The stage table's rows in stored order, or None when there is no such table."""
        name = ".".join(stage_table(stage, self.source, self.dataset))
        if isinstance(self.loader, MemoryLoader):
            found = {
                Stage.RAW: self.loader.raw,
                Stage.STAGING: self.loader.staging,
                Stage.CLEAN: self.loader.clean,
            }[stage].get(name)
            return None if found is None else found.clone()
        assert self.reader is not None
        identifier = sql.Identifier(*stage_table(stage, self.source, self.dataset))
        exists = self.reader.execute("SELECT to_regclass(%s) IS NOT NULL", [name]).fetchone()
        if not (exists and exists[0]):
            return None
        cursor = self.reader.execute(sql.SQL("SELECT * FROM {} ORDER BY ctid").format(identifier))
        columns = [column.name for column in cursor.description or ()]
        # A uuid as its text: polars would hold it as an object, which no two frames compare equal.
        rows = [
            tuple(str(value) if isinstance(value, UUID) else value for value in row)
            for row in cursor.fetchall()
        ]
        return pl.DataFrame(rows, schema=columns, orient="row", infer_schema_length=None)

    def record(self, execution_id: UUID) -> PipelineRun:
        with self.loader.stages() as stages:
            found = read_run(stages, execution_id)
        assert found is not None
        return found

    def drop(self) -> None:
        if self.reader is None:
            return
        for stage in Stage:
            identifier = sql.Identifier(*stage_table(stage, self.source, self.dataset))
            self.reader.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(identifier))


@pytest.fixture(params=["memory", pytest.param("postgres", marks=pytest.mark.db)])
def shop(request: pytest.FixtureRequest) -> Iterator[Shop]:
    if request.param == "memory":
        yield Shop(MemoryLoader(), None)
        return
    url = Settings().database_url  # type: ignore[call-arg]
    with PostgresLoader(url) as loader, psycopg.connect(url, autocommit=True) as reader:
        made = Shop(loader, reader)
        try:
            yield made
        finally:
            made.drop()


def _frames_equal(left: pl.DataFrame | None, right: pl.DataFrame | None) -> bool:
    if left is None or right is None:
        return left is right
    return left.equals(right)


# --- the four shapes of a run ------------------------------------------------------------------


def test_a_pipeline_runs_end_to_end_from_raw_to_clean(shop: Shop) -> None:
    shop.ingest()

    run = shop.run(shop.pipeline())

    assert (run.status, run.rows_in, run.rows_out, run.error) == ("succeeded", 4, 3, None)
    clean = shop.table(Stage.CLEAN)
    assert clean is not None
    assert sorted(clean.select("id", "name", "email").rows()) == [
        (1, "ada", "a@x.nl"),
        (3, "cy", "c@x.nl"),
        (4, None, "d@x.nl"),
    ]
    assert shop.table(Stage.STAGING) is None
    trim, drop = run.steps
    assert (trim.type, trim.status, trim.rows_in, trim.rows_out, trim.values_changed) == (
        "normalize_values",
        "succeeded",
        4,
        4,
        1,
    )
    assert (drop.status, drop.rows_in, drop.rows_out) == ("succeeded", 4, 3)
    assert [(node.kind, node.step_position) for node in run.lineage] == [
        ("raw", None),
        ("step", 1),
        ("step", 2),
        ("clean", None),
    ]
    assert run.lineage[1].configuration == trim.configuration
    assert [(check.constraint, check.passed) for check in run.validation] == [("not_null", True)]


def test_a_run_that_fails_at_a_middle_step_names_it_and_leaves_clean_as_it_was(
    shop: Shop, tmp_path: Path
) -> None:
    shop.ingest()
    shop.run(shop.pipeline())
    clean_before = shop.table(Stage.CLEAN)
    script = tmp_path / "broken.py"
    script.write_text(
        "def transform(df):\n    raise ValueError('cannot read row 3')\n", encoding="utf-8"
    )
    steps = [TIDY[0], {"type": "python", "script": script.as_posix()}, TIDY[1]]

    run = shop.run(shop.pipeline(steps, name="broken"))

    assert run.status == "failed"
    assert run.failed_step == 2
    assert run.error is not None
    assert run.error.startswith("step 2 (python) with {")
    assert script.as_posix() in run.error
    assert "cannot read row 3" in run.error
    assert [step.status for step in run.steps] == ["succeeded", "failed", "not_run"]
    assert run.steps[1].error_line == 2
    assert run.rows_out is None
    assert _frames_equal(shop.table(Stage.CLEAN), clean_before)
    assert shop.table(Stage.STAGING) is None
    assert [node.kind for node in run.lineage] == ["raw", "step", "step"]


def test_a_run_that_breaks_a_critical_constraint_fails_and_leaves_clean_as_it_was(
    shop: Shop,
) -> None:
    shop.ingest()
    shop.run(shop.pipeline())
    clean_before = shop.table(Stage.CLEAN)
    email_required = {"constraint": "not_null", "column": "email", "critical": True}
    unknown_but_mild = {"constraint": "max", "column": "id", "value": 3}

    run = shop.run(
        shop.pipeline(TIDY[:1], name="strict", constraints=[unknown_but_mild, email_required])
    )

    assert (run.status, run.failed_step) == ("failed", None)
    assert run.error == (
        "validation: critical constraint 2 (not_null on email): 1 row and 1 value break it"
    )
    assert [(check.constraint, check.critical, check.passed) for check in run.validation] == [
        ("max", False, False),
        ("not_null", True, False),
    ]
    assert [step.status for step in run.steps] == ["succeeded"]
    assert _frames_equal(shop.table(Stage.CLEAN), clean_before)
    assert shop.table(Stage.STAGING) is None


def test_a_pipeline_with_no_steps_publishes_raws_rows_as_clean(shop: Shop) -> None:
    shop.ingest()

    run = shop.run(shop.pipeline([], name="copy"))

    assert (run.status, run.steps, run.rows_in, run.rows_out) == ("succeeded", [], 4, 4)
    clean = shop.table(Stage.CLEAN)
    assert clean is not None
    assert clean.columns == ["id", "name", "email"]
    assert sorted(clean.rows(), key=lambda row: row[0]) == sorted(ORDERS.rows())
    assert [node.kind for node in run.lineage] == ["raw", "clean"]
    assert [profile.stage for profile in run.profiles] == ["raw", "clean"]


# --- what a run keeps and records --------------------------------------------------------------


def test_two_runs_of_one_version_make_the_same_clean_table(shop: Shop) -> None:
    shop.ingest()
    pipeline = shop.pipeline()

    first = shop.run(pipeline)
    clean_first = shop.table(Stage.CLEAN)
    second = shop.run(pipeline)

    assert (first.version, second.version) == (1, 1)
    assert first.execution_id != second.execution_id
    assert _frames_equal(shop.table(Stage.CLEAN), clean_first)


VALUES = st.one_of(st.none(), st.text(alphabet=" ab", max_size=3))
ROWS = st.lists(st.tuples(st.integers(-3, 3), VALUES, VALUES), min_size=0, max_size=12)


@settings(suppress_health_check=[HealthCheck.too_slow], max_examples=60)
@given(rows=ROWS, seed=st.randoms(use_true_random=False))
def test_the_same_rows_give_the_same_clean_table_whatever_order_raw_holds_them_in(
    rows: list[tuple[int, str | None, str | None]], seed: Any
) -> None:
    frame = pl.DataFrame(
        rows,
        schema={"id": pl.Int64, "name": pl.String, "email": pl.String},
        orient="row",
    )
    shuffled = frame.sample(fraction=1.0, shuffle=True, seed=seed.randint(0, 2**32 - 1))
    cleans = []
    for given_rows in (frame, shuffled):
        shop = Shop(MemoryLoader(), None)
        shop.source = "same"
        shop.ingest(given_rows)
        run = shop.run(shop.pipeline(constraints=(), profile="none"))
        assert run.status == "succeeded", run.error
        cleans.append(shop.table(Stage.CLEAN))

    assert _frames_equal(*cleans)


def test_a_finished_runs_record_has_every_field(shop: Shop) -> None:
    shop.ingest()
    finished = shop.run(shop.pipeline(profile="every_step"))

    record = shop.record(finished.execution_id).model_dump()

    assert record["pipeline"] == "tidy"
    assert isinstance(record["pipeline_id"], int)
    assert record["version"] == 1
    assert (record["source"], record["dataset"]) == (shop.source, shop.dataset)
    assert NOW < record["started_at"] < record["ended_at"]
    assert record["status"] == "succeeded"
    assert (record["rows_in"], record["rows_out"]) == (4, 3)
    assert [(step["position"], step["status"]) for step in record["steps"]] == [
        (1, "succeeded"),
        (2, "succeeded"),
    ]
    assert all(step["duration_seconds"] is not None for step in record["steps"])
    assert [(p["stage"], p["after_step"], p["table_rows"]) for p in record["profiles"]] == [
        ("raw", None, 4),
        ("staging", 1, 4),
        ("staging", 2, 3),
        ("clean", None, 3),
    ]
    assert record["profiles"][0]["missing_values"] == 2
    assert record["profiles"][-1]["missing_values"] == 1
    assert [(v["constraint"], v["columns"], v["passed"]) for v in record["validation"]] == [
        ("not_null", ["id"], True)
    ]
    assert record["steps"][0]["configuration"]["columns"] == ["name"]


def test_profiles_are_taken_only_where_the_pipeline_says(shop: Shop) -> None:
    shop.ingest()

    none = shop.run(shop.pipeline(profile="none"))
    ends = shop.run(shop.pipeline(profile="ends", name="ends"))

    assert none.profiles == []
    assert [(p.stage, p.after_step) for p in ends.profiles] == [("raw", None), ("clean", None)]


def test_editing_a_pipeline_makes_a_new_version_and_the_earlier_run_keeps_its_own(
    shop: Shop,
) -> None:
    shop.ingest()
    first = shop.run(shop.pipeline(TIDY[:1]))
    unchanged = shop.run(shop.pipeline(TIDY[:1]))

    edited = shop.run(shop.pipeline(TIDY))

    assert (first.version, unchanged.version, edited.version) == (1, 1, 2)
    assert first.pipeline_id == edited.pipeline_id
    again = shop.record(first.execution_id)
    assert again.version == 1
    assert [step.type for step in again.steps] == ["normalize_values"]
    assert [step.type for step in shop.record(edited.execution_id).steps] == [
        "normalize_values",
        "drop_missing",
    ]


def test_raw_is_unchanged_by_a_run_whether_it_succeeds_or_fails(shop: Shop) -> None:
    shop.ingest()
    raw_before = shop.table(Stage.RAW)

    succeeded = shop.run(shop.pipeline())
    failed = shop.run(
        shop.pipeline([{"type": "fill_missing", "columns": ["gone"], "method": "mode"}], name="x")
    )

    assert (succeeded.status, failed.status) == ("succeeded", "failed")
    assert failed.error is not None and "column 'gone' is not in the data" in failed.error
    assert _frames_equal(shop.table(Stage.RAW), raw_before)


# --- which rows a run starts from ---------------------------------------------------------------


def test_a_full_load_starts_from_the_newest_ingest_only(shop: Shop) -> None:
    shop.ingest()
    shop.ingest(ORDERS.head(2))

    run = shop.run(shop.pipeline([], load_mode="full"))

    assert (run.rows_in, run.rows_out) == (2, 2)


def test_a_merge_load_starts_from_the_newest_row_of_each_key(shop: Shop) -> None:
    shop.ingest()
    shop.ingest(pl.DataFrame({"id": [2], "name": ["bo again"], "email": ["b@x.nl"]}))

    run = shop.run(shop.pipeline([], load_mode="merge", primary_key=["id"]))

    clean = shop.table(Stage.CLEAN)
    assert clean is not None
    assert run.rows_in == 4
    assert clean.filter(pl.col("id") == 2).rows() == [(2, "bo again", "b@x.nl")]


def test_a_dataset_with_nothing_in_raw_fails_the_run_saying_what_to_do(shop: Shop) -> None:
    run = shop.run(shop.pipeline(load_mode="full"))

    assert run.status == "failed"
    assert run.error is not None and f"run `udp run {shop.source}` first" in run.error


# --- one run of a dataset at a time -------------------------------------------------------------


def test_a_second_run_of_a_running_pipeline_is_refused_naming_it(shop: Shop) -> None:
    shop.ingest()
    pipeline = shop.pipeline()
    started = start_pipeline(shop.loader, pipeline, clock=shop.now)

    with pytest.raises(PipelineBusy, match=str(started.execution_id)) as refused:
        run_pipeline(shop.loader, pipeline, clock=shop.now)

    assert refused.value.execution_id == started.execution_id
    assert "pipeline 'tidy' version 1" in str(refused.value)
    assert carry_out(shop.loader, started, clock=shop.now).status == "succeeded"
    assert shop.run(pipeline).status == "succeeded"


def test_a_pipeline_waits_for_an_ingest_run_of_its_dataset(shop: Shop) -> None:
    shop.ingest()
    assert shop.loader.lock_dataset(shop.source, shop.dataset)

    with pytest.raises(PipelineBusy, match="being loaded by an ingest run"):
        shop.run(shop.pipeline())

    shop.loader.unlock_dataset(shop.source, shop.dataset)
    assert shop.run(shop.pipeline()).status == "succeeded"


def test_a_run_whose_process_died_is_marked_interrupted_by_the_next(shop: Shop) -> None:
    shop.ingest()
    dead = start_pipeline(shop.loader, shop.pipeline(), clock=shop.now)
    shop.loader.unlock_dataset(shop.source, shop.dataset)

    next_run = shop.run(shop.pipeline())

    cut_off = shop.record(dead.execution_id)
    assert (cut_off.status, cut_off.error_class) == ("failed", INTERRUPTED)
    assert cut_off.error is not None and str(next_run.execution_id) in cut_off.error
    assert next_run.status == "succeeded"


# --- the demo pipeline, from the command line ---------------------------------------------------


@pytest.mark.db
def test_the_demo_pipeline_runs_over_demo_csv_from_the_cli() -> None:
    runner = CliRunner()
    env = {"UDP_SOURCES_DIR": "sources"}
    loaded = runner.invoke(app, ["run", "demo_csv"], env=env)
    assert loaded.exit_code == 0, loaded.output[-3000:]
    url = Settings().database_url  # type: ignore[call-arg]
    raw = (
        "SELECT count(*), md5(string_agg(t::text, ',' ORDER BY t::text)) "
        "FROM raw.demo_csv__customers t"
    )
    with psycopg.connect(url) as conn:
        raw_before = conn.execute(raw).fetchone()

    ran = runner.invoke(app, ["pipeline", "run", "demo_csv_customers"], env=env)

    assert ran.exit_code == 0, ran.output[-3000:]
    found = re.search(r"^Run ([0-9a-f-]{36}) ", ran.output, re.MULTILINE)
    assert found is not None, ran.output
    shown = runner.invoke(app, ["pipeline", "status", found[1], "--json"], env=env)
    assert shown.exit_code == 0, shown.output[-3000:]
    record = PipelineRun.model_validate_json(shown.output)
    assert (record.status, record.pipeline, record.dataset) == (
        "succeeded",
        "demo_csv_customers",
        "customers",
    )
    assert record.rows_in == record.rows_out == 20
    assert all(check.passed for check in record.validation if check.critical)
    with psycopg.connect(url) as conn:
        assert conn.execute(raw).fetchone() == raw_before
        filled = conn.execute(
            "SELECT count(*) FROM clean.demo_csv__customers WHERE lifetime_value IS NULL"
        ).fetchone()
    assert filled == (0,)
