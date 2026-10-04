"""RAW, STAGING and CLEAN, and the V2 records, against a real PostgreSQL.

The pipeline these tests run is a stand-in that lives only here: it copies RAW into STAGING,
changes STAGING with plain SQL, and finishes. Running real pipelines is the execution engine's.
"""

import uuid
from collections.abc import Iterator
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import polars as pl
import psycopg
import pytest
from psycopg import errors, sql

from udp.errors import LoadError, SchemaDriftError
from udp.names import Stage, stage_table
from udp.pipeline.load import with_platform_columns
from udp.settings import Settings
from udp.storage.loader import (
    ConstraintResult,
    ExecutionStart,
    LineageNode,
    Profile,
    RunFailure,
    RunRef,
    RunStart,
    StepDefinition,
    StepRun,
    Violation,
    stage_node,
)
from udp.storage.postgres import PostgresLoader, PostgresStages

pytestmark = pytest.mark.db

NOW = datetime(2026, 10, 4, 12, tzinfo=UTC)
ORDERS = pl.DataFrame(
    {
        "id": [1, 2, 3, 4],
        "name": ["  ada ", "bo", None, "cy"],
        "email": ["a@x.nl", None, "c@x.nl", "d@x.nl"],
    }
)
STEPS = (
    StepDefinition("trim", {"column": "name"}),
    StepDefinition("drop_missing", {"column": "email"}),
)


class Shop:
    """One throwaway dataset with its stage tables, and the stand-in pipeline that runs over it."""

    def __init__(self, loader: PostgresLoader, reader: psycopg.Connection) -> None:
        self.loader = loader
        self.reader = reader
        self.source = f"stg{uuid.uuid4().hex[:10]}"
        self.dataset = "orders"

    def table(self, stage: Stage) -> sql.Identifier:
        return sql.Identifier(*stage_table(stage, self.source, self.dataset))

    def stages(self) -> AbstractContextManager[PostgresStages]:
        return self.loader.stages()

    def ingest(self, frame: pl.DataFrame = ORDERS) -> UUID:
        """An ingest run that appends the frame to RAW, recorded as a succeeded run."""
        run = RunStart(uuid.uuid7(), self.source, self.dataset, "manual", NOW)
        self.loader.start_run(run)
        with self.stages() as stages:
            stages.append_raw(
                self.source, self.dataset, [with_platform_columns(frame, run.run_id, NOW)]
            )
        with self.loader.transaction() as transaction:
            transaction.succeed_run(
                run.run_id, ended_at=NOW, rows_extracted=frame.height, rows_loaded=frame.height
            )
        return run.run_id

    def fingerprint(self, stage: Stage) -> tuple[int, str]:
        """The table's row count and an md5 of every row's text, in a fixed order."""
        row = self.reader.execute(
            sql.SQL(
                "SELECT count(*), md5(coalesce(string_agg(t::text, ',' ORDER BY t::text), '')) "
                "FROM {} AS t"
            ).format(self.table(stage))
        ).fetchone()
        assert row is not None
        return int(row[0]), str(row[1])

    def rows(self, stage: Stage) -> list[tuple[Any, ...]]:
        return self.reader.execute(
            sql.SQL("SELECT id, name, email FROM {} ORDER BY id").format(self.table(stage))
        ).fetchall()

    def exists(self, stage: Stage) -> bool:
        with self.stages() as stages:
            return stages.exists(stage, self.source, self.dataset)

    def start(self, steps: tuple[StepDefinition, ...] = STEPS) -> tuple[UUID, int]:
        """Save the stand-in pipeline, start an execution of it and copy RAW into STAGING."""
        execution_id = uuid.uuid7()
        with self.stages() as stages:
            pipeline = stages.save_pipeline(
                self.source, self.dataset, "tidy", {"steps": len(steps)}, steps, NOW
            )
            stages.start_execution(
                ExecutionStart(execution_id, pipeline.pipeline_id, pipeline.version, "manual", NOW)
            )
            copied = stages.start_staging(self.source, self.dataset)
        return execution_id, copied

    def step(self, execution_id: UUID, position: int, statement: str) -> None:
        """One stand-in step: plain SQL over STAGING, recorded as running and then succeeded."""
        with self.stages() as stages:
            stages.record_step(execution_id, StepRun(position, "running", NOW))
        changed = self.reader.execute(
            sql.SQL(statement).format(table=self.table(Stage.STAGING))
        ).rowcount
        with self.stages() as stages:
            stages.record_step(
                execution_id,
                StepRun(
                    position, "succeeded", NOW, NOW + timedelta(seconds=1), values_changed=changed
                ),
            )

    def run_pipeline(self) -> UUID:
        execution_id, copied = self.start()
        self.step(execution_id, 1, "UPDATE {table} SET name = trim(name) WHERE name <> trim(name)")
        self.step(execution_id, 2, "DELETE FROM {table} WHERE email IS NULL")
        with self.stages() as stages:
            stages.finish_execution(
                execution_id, ended_at=NOW, rows_in=copied, rows_out=len(self.rows(Stage.STAGING))
            )
        return execution_id

    def drop(self) -> None:
        for stage in Stage:
            self.reader.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(self.table(stage)))


@pytest.fixture
def shop() -> Iterator[Shop]:
    url = Settings().database_url  # type: ignore[call-arg]
    with PostgresLoader(url) as loader, psycopg.connect(url, autocommit=True) as reader:
        made = Shop(loader, reader)
        try:
            yield made
        finally:
            made.drop()


def test_a_pipeline_run_leaves_raw_byte_for_byte_unchanged(shop: Shop) -> None:
    shop.ingest()
    shop.ingest(ORDERS.with_columns(pl.col("id") + 10))
    before = shop.fingerprint(Stage.RAW)

    execution_id = shop.run_pipeline()

    assert shop.fingerprint(Stage.RAW) == before
    assert before[0] == 8
    assert shop.rows(Stage.CLEAN) == [
        (1, "ada", "a@x.nl"),
        (3, None, "c@x.nl"),
        (4, "cy", "d@x.nl"),
        (11, "ada", "a@x.nl"),
        (13, None, "c@x.nl"),
        (14, "cy", "d@x.nl"),
    ]
    assert not shop.exists(Stage.STAGING)
    with shop.stages() as stages:
        execution = stages.read_execution(execution_id)
    assert execution is not None
    assert (execution.status, execution.rows_in, execution.rows_out) == ("succeeded", 8, 6)
    assert [(step.position, step.status, step.values_changed) for step in execution.steps] == [
        (1, "succeeded", 2),
        (2, "succeeded", 2),
    ]


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE {table} SET name = 'changed'",
        "UPDATE {table} SET name = name WHERE false",
        "DELETE FROM {table} WHERE id = 1",
        "TRUNCATE {table}",
    ],
)
def test_raw_refuses_every_change_to_what_was_ingested(shop: Shop, statement: str) -> None:
    shop.ingest()
    before = shop.fingerprint(Stage.RAW)

    with pytest.raises(errors.RestrictViolation, match="append-only"):
        shop.reader.execute(sql.SQL(statement).format(table=shop.table(Stage.RAW)))

    assert shop.fingerprint(Stage.RAW) == before


def test_raw_takes_new_rows_and_new_columns_but_never_a_new_type(shop: Shop) -> None:
    first = shop.ingest()
    second = shop.ingest(ORDERS.with_columns(pl.lit("NL").alias("country")))

    runs = shop.reader.execute(
        sql.SQL("SELECT _run_id, count(*), count(country) FROM {} GROUP BY 1").format(
            shop.table(Stage.RAW)
        )
    ).fetchall()
    assert sorted(runs) == sorted([(first, 4, 0), (second, 4, 4)])
    with pytest.raises(SchemaDriftError, match=r"raw\..*RAW keeps what was ingested"):
        shop.ingest(ORDERS.with_columns(pl.col("id").cast(pl.String)))
    with pytest.raises(LoadError, match="_run_id"), shop.stages() as stages:
        stages.append_raw(shop.source, shop.dataset, [ORDERS])


def test_staging_can_start_from_chosen_ingest_runs_only(shop: Shop) -> None:
    first = shop.ingest()
    shop.ingest(ORDERS.with_columns(pl.col("id") + 10))

    with shop.stages() as stages:
        copied = stages.start_staging(shop.source, shop.dataset, ingest_runs=[first])

    assert copied == 4
    assert [row[0] for row in shop.rows(Stage.STAGING)] == [1, 2, 3, 4]


def test_staging_needs_raw(shop: Shop) -> None:
    with pytest.raises(LoadError, match="no RAW table"), shop.stages() as stages:
        stages.start_staging(shop.source, shop.dataset)


def test_a_failed_execution_drops_staging_and_leaves_clean_as_it_was(shop: Shop) -> None:
    shop.ingest()
    shop.run_pipeline()
    clean = shop.fingerprint(Stage.CLEAN)
    execution_id, _ = shop.start()
    shop.reader.execute(sql.SQL("DELETE FROM {}").format(shop.table(Stage.STAGING)))

    with shop.stages() as stages:
        stages.record_step(
            execution_id, StepRun(1, "failed", NOW, NOW, error_class="E", error_message="boom")
        )
        stages.finish_execution(
            execution_id,
            ended_at=NOW,
            rows_in=4,
            rows_out=None,
            failure=RunFailure("TransformError", "step 1 failed: boom", "trace"),
            failed_step=1,
        )

    assert shop.fingerprint(Stage.CLEAN) == clean
    assert not shop.exists(Stage.STAGING)
    with shop.stages() as stages:
        execution = stages.read_execution(execution_id)
    assert execution is not None
    assert (execution.status, execution.failed_step, execution.error_message) == (
        "failed",
        1,
        "step 1 failed: boom",
    )
    assert execution.steps[0].error_message == "boom"


def test_an_execution_ends_once_and_publishes_only_a_staging_table(shop: Shop) -> None:
    shop.ingest()
    execution_id, _ = shop.start()
    shop.reader.execute(sql.SQL("DROP TABLE {}").format(shop.table(Stage.STAGING)))

    with pytest.raises(LoadError, match="no STAGING table"), shop.stages() as stages:
        stages.finish_execution(execution_id, ended_at=NOW, rows_in=4, rows_out=4)
    with shop.stages() as stages:
        stages.finish_execution(
            execution_id, ended_at=NOW, rows_in=4, rows_out=None, failure=RunFailure("E", "m", "t")
        )
    with pytest.raises(LoadError, match="not running"), shop.stages() as stages:
        stages.finish_execution(execution_id, ended_at=NOW, rows_in=4, rows_out=4)
    with pytest.raises(ValueError, match="only a failed execution"), shop.stages() as stages:
        stages.finish_execution(execution_id, ended_at=NOW, rows_in=4, rows_out=4, failed_step=1)


def test_a_step_record_ends_once_and_must_be_a_step_of_the_pipeline(shop: Shop) -> None:
    shop.ingest()
    execution_id, _ = shop.start()
    shop.step(execution_id, 1, "UPDATE {table} SET name = trim(name)")

    with pytest.raises(LoadError, match="already ended"), shop.stages() as stages:
        stages.record_step(execution_id, StepRun(1, "running", NOW))
    with pytest.raises(LoadError, match="no pipeline with a step 3"), shop.stages() as stages:
        stages.record_step(execution_id, StepRun(3, "running", NOW))


def test_editing_a_pipeline_adds_a_version_and_keeps_the_old_one(shop: Shop) -> None:
    def save(stages: PostgresStages, steps: tuple[StepDefinition, ...]) -> Any:
        return stages.save_pipeline(
            shop.source, shop.dataset, "tidy", {"note": ("a", 1.5)}, steps, NOW
        )

    with shop.stages() as stages:
        first = save(stages, STEPS)
        again = save(stages, STEPS)
        edited = save(stages, (*STEPS, StepDefinition("dedupe", {"columns": ["id"]})))
        old = stages.read_pipeline(shop.source, shop.dataset, "tidy", 1)
        missing = stages.read_pipeline(shop.source, shop.dataset, "other")

    assert (first.version, again.version, edited.version) == (1, 1, 2)
    assert again == first
    assert old == first
    assert old is not None and old.steps == STEPS
    assert edited.steps[-1] == StepDefinition("dedupe", {"columns": ["id"]})
    assert missing is None


def test_profiles_are_kept_per_stage_per_run(shop: Shop) -> None:
    shop.ingest()
    first = shop.run_pipeline()
    later = shop.run_pipeline()

    def profile(stage: Stage, run: UUID, rows: int) -> Profile:
        return Profile(
            shop.source, shop.dataset, stage, RunRef(execution_id=run), rows, {"rows": rows}, NOW
        )

    written = [profile(Stage.RAW, first, 4), profile(Stage.CLEAN, first, 3)]
    written.append(profile(Stage.CLEAN, later, 3))
    with shop.stages() as stages:
        ids = [stages.record_profile(item) for item in written]
        every = stages.read_profiles(shop.source, shop.dataset)
        clean = stages.read_profiles(shop.source, shop.dataset, Stage.CLEAN)

    assert len(set(ids)) == 3
    assert [stored.profile_id for stored in every] == ids
    assert [stored.profile for stored in every] == written
    assert [stored.profile.run.execution_id for stored in clean] == [first, later]


def test_a_profile_can_follow_a_step_or_belong_to_an_ingest_run(shop: Shop) -> None:
    ingest = shop.ingest()
    execution_id, _ = shop.start()
    after = Profile(
        shop.source,
        shop.dataset,
        Stage.STAGING,
        RunRef(execution_id=execution_id),
        4,
        {},
        NOW,
        after_step=1,
    )
    of_raw = Profile(shop.source, shop.dataset, Stage.RAW, RunRef(ingest), 4, {}, NOW)

    with shop.stages() as stages:
        stages.record_profile(after)
        stages.record_profile(of_raw)
        stored = [item.profile for item in stages.read_profiles(shop.source, shop.dataset)]

    assert stored == [after, of_raw]


def test_the_database_refuses_a_result_of_no_run_or_of_two(shop: Shop) -> None:
    ingest = shop.ingest()
    execution_id, _ = shop.start()
    insert = (
        "INSERT INTO platform.profiles (source, dataset, stage, ingest_run_id, execution_id, "
        "table_rows, result, profiled_at) VALUES (%s, %s, 'raw', %s, %s, 0, '{}', now())"
    )

    for ingest_run_id, execution in [(None, None), (ingest, execution_id)]:
        with pytest.raises(errors.CheckViolation):
            shop.reader.execute(insert, [shop.source, shop.dataset, ingest_run_id, execution])


def test_constraint_results_read_back_with_their_violations(shop: Shop) -> None:
    shop.ingest()
    execution_id = shop.run_pipeline()
    run = RunRef(execution_id=execution_id)
    broken = ConstraintResult(
        shop.source,
        shop.dataset,
        Stage.CLEAN,
        run,
        1,
        "not_null",
        ("name",),
        True,
        False,
        2,
        2,
        "2 rows have no name",
        {"column": "name"},
        NOW,
        violations=(Violation("name", {"id": 3}, None), Violation("name", {"id": 13}, None)),
    )
    held = ConstraintResult(
        shop.source,
        shop.dataset,
        Stage.RAW,
        run,
        2,
        "unique",
        ("id",),
        False,
        True,
        0,
        0,
        "",
        {},
        NOW,
    )

    with shop.stages() as stages:
        stages.record_constraint_results([broken, held])
        every = stages.read_constraint_results(shop.source, shop.dataset)
        raw = stages.read_constraint_results(shop.source, shop.dataset, Stage.RAW)

    assert every == [broken, held]
    assert raw == [held]


def test_lineage_links_the_source_to_raw_and_raw_through_the_steps_to_clean(shop: Shop) -> None:
    ingest = RunRef(ingest_run_id=shop.ingest())
    execution = RunRef(execution_id=shop.run_pipeline())
    raw = stage_node(Stage.RAW, shop.source, shop.dataset)
    clean = stage_node(Stage.CLEAN, shop.source, shop.dataset)
    origin = LineageNode("source", "sources/shop/data/orders.csv")
    steps = [LineageNode("step", "trim", 1), LineageNode("step", "drop_missing", 2)]

    with shop.stages() as stages:
        stages.record_lineage(shop.source, shop.dataset, ingest, [origin, raw], NOW)
        stages.record_lineage(shop.source, shop.dataset, execution, [raw], NOW)
        stages.record_lineage(shop.source, shop.dataset, execution, steps[:1], NOW)
        stages.record_lineage(shop.source, shop.dataset, execution, [*steps[1:], clean], NOW)
    with pytest.raises(ValueError, match="cannot come after clean"), shop.stages() as stages:
        stages.record_lineage(shop.source, shop.dataset, execution, [raw], NOW)
    with shop.stages() as stages:
        from_ingest = stages.read_lineage(ingest)
        from_execution = stages.read_lineage(execution)

    assert from_ingest == (origin, raw)
    assert from_execution == (raw, *steps, clean)
    assert raw.name == f"raw.{shop.source}__orders"
