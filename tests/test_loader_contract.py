import math
import uuid
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import polars as pl
import psycopg
import pytest
from fakes import MemoryLoader
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from psycopg import sql
from psycopg.rows import dict_row

from udp.errors import LoadError
from udp.settings import Settings
from udp.storage.loader import Loader, RunFailure, RunStart
from udp.storage.postgres import PostgresLoader


@dataclass
class Harness:
    loader: Loader
    read_rows: Callable[[str, list[str]], list[tuple[Any, ...]]]
    read_run: Callable[[UUID], dict[str, Any]]
    drop_table: Callable[[str], None]


def _memory_harness() -> Iterator[Harness]:
    loader = MemoryLoader()

    def drop_table(table: str) -> None:
        loader.tables.pop(table, None)

    yield Harness(
        loader=loader,
        read_rows=lambda table, columns: loader.tables[table].select(columns).rows(),
        read_run=lambda run_id: loader.runs[run_id],
        drop_table=drop_table,
    )


def _postgres_harness() -> Iterator[Harness]:
    url = Settings().database_url  # type: ignore[call-arg]
    with PostgresLoader(url) as loader, psycopg.connect(url, autocommit=True) as reader:

        def read_rows(table: str, columns: list[str]) -> list[tuple[Any, ...]]:
            query = sql.SQL("SELECT {} FROM {}").format(
                sql.SQL(", ").join(sql.Identifier(c) for c in columns),
                sql.Identifier("datasets", table),
            )
            return reader.execute(query).fetchall()

        def read_run(run_id: UUID) -> dict[str, Any]:
            with reader.cursor(row_factory=dict_row) as cursor:
                row = cursor.execute(
                    "SELECT * FROM platform.pipeline_runs WHERE run_id = %s", [run_id]
                ).fetchone()
            assert row is not None
            return row

        def drop_table(table: str) -> None:
            target = sql.Identifier("datasets", table)
            reader.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(target))

        yield Harness(loader, read_rows, read_run, drop_table)


@pytest.fixture(params=["memory", pytest.param("postgres", marks=pytest.mark.db)])
def harness(request: pytest.FixtureRequest) -> Iterator[Harness]:
    yield from _memory_harness() if request.param == "memory" else _postgres_harness()


def _new_table() -> str:
    return f"contract__t{uuid.uuid4().hex[:12]}"


def _start_run(loader: Loader) -> UUID:
    run_id = uuid.uuid7()
    loader.start_run(RunStart(run_id, "contract", "t", "manual", datetime.now(UTC)))
    return run_id


def _comparable(rows: list[tuple[Any, ...]]) -> Counter[tuple[Any, ...]]:
    def normalise(value: Any) -> Any:
        return "NaN" if isinstance(value, float) and math.isnan(value) else value

    return Counter(tuple(normalise(value) for value in row) for row in rows)


def _replace(harness: Harness, table: str, *frames: pl.DataFrame) -> int:
    with harness.loader.transaction() as transaction:
        return transaction.replace_table(table, iter(frames))


FIRST = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", None]})
SECOND = pl.DataFrame({"id": [9, 8], "name": ["x", ""]})


def test_replace_loads_every_row(harness: Harness) -> None:
    table = _new_table()

    loaded = _replace(harness, table, FIRST.head(2), FIRST.tail(1))

    assert loaded == 3
    assert _comparable(harness.read_rows(table, FIRST.columns)) == _comparable(FIRST.rows())
    harness.drop_table(table)


def test_second_replace_leaves_only_the_second_rows(harness: Harness) -> None:
    table = _new_table()
    _replace(harness, table, FIRST)

    loaded = _replace(harness, table, SECOND)

    assert loaded == 2
    assert _comparable(harness.read_rows(table, SECOND.columns)) == _comparable(SECOND.rows())
    harness.drop_table(table)


def test_different_columns_fail_and_keep_the_table(harness: Harness) -> None:
    table = _new_table()
    _replace(harness, table, FIRST)

    with pytest.raises(LoadError, match="columns"):
        _replace(harness, table, FIRST.with_columns(pl.col("id").cast(pl.String)))

    assert _comparable(harness.read_rows(table, FIRST.columns)) == _comparable(FIRST.rows())
    harness.drop_table(table)


def test_chunks_failing_after_the_first_keep_the_previous_rows(harness: Harness) -> None:
    table = _new_table()
    _replace(harness, table, FIRST)

    def broken() -> Iterator[pl.DataFrame]:
        yield SECOND
        raise RuntimeError("source went away")

    with pytest.raises(RuntimeError), harness.loader.transaction() as transaction:
        transaction.replace_table(table, broken())

    assert _comparable(harness.read_rows(table, FIRST.columns)) == _comparable(FIRST.rows())
    harness.drop_table(table)


def test_unknown_run_rolls_back_the_replace_in_the_same_transaction(harness: Harness) -> None:
    table = _new_table()
    _replace(harness, table, FIRST)

    with pytest.raises(LoadError, match="not a running run"), harness.loader.transaction() as tx:
        tx.replace_table(table, iter([SECOND]))
        tx.succeed_run(uuid.uuid7(), ended_at=datetime.now(UTC), rows_extracted=2, rows_loaded=2)

    assert _comparable(harness.read_rows(table, FIRST.columns)) == _comparable(FIRST.rows())
    harness.drop_table(table)


def test_failing_a_run_that_is_not_running_raises(harness: Harness) -> None:
    finished = _start_run(harness.loader)
    failure = RunFailure("ExtractError", "file not found", "Traceback ...")
    harness.loader.fail_run(finished, ended_at=datetime.now(UTC), rows_extracted=0, failure=failure)

    for run_id in (uuid.uuid7(), finished):
        with pytest.raises(LoadError, match="not a running run"):
            harness.loader.fail_run(
                run_id, ended_at=datetime.now(UTC), rows_extracted=0, failure=failure
            )


def test_runs_record_success_and_failure(harness: Harness) -> None:
    table = _new_table()
    succeeded = _start_run(harness.loader)
    failed = _start_run(harness.loader)
    assert harness.read_run(succeeded)["status"] == "running"

    with harness.loader.transaction() as transaction:
        loaded = transaction.replace_table(table, iter([FIRST]))
        transaction.succeed_run(
            succeeded, ended_at=datetime.now(UTC), rows_extracted=3, rows_loaded=loaded
        )
    harness.loader.fail_run(
        failed,
        ended_at=datetime.now(UTC),
        rows_extracted=None,
        failure=RunFailure("ExtractError", "file not found", "Traceback ..."),
    )

    ok = harness.read_run(succeeded)
    assert (ok["status"], ok["rows_extracted"], ok["rows_loaded"]) == ("succeeded", 3, 3)
    assert ok["started_at"] <= ok["ended_at"]
    bad = harness.read_run(failed)
    assert (bad["status"], bad["error_class"], bad["error_message"]) == (
        "failed",
        "ExtractError",
        "file not found",
    )
    harness.drop_table(table)


text = st.one_of(
    st.text(alphabet=st.characters(blacklist_characters="\x00", blacklist_categories=["Cs"])),
    st.sampled_from(["", "\\N", 'a"b', "a,b", "x\ny", "x\r\ny", "\t", "\\", "\\.", "NULL"]),
)

COLUMN_KINDS: dict[str, tuple[pl.DataType, st.SearchStrategy[Any]]] = {
    "int16": (pl.Int16(), st.integers(-(2**15), 2**15 - 1)),
    "int32": (pl.Int32(), st.integers(-(2**31), 2**31 - 1)),
    "int64": (pl.Int64(), st.integers(-(2**63), 2**63 - 1)),
    "float32": (pl.Float32(), st.floats(width=32)),
    "float64": (pl.Float64(), st.floats()),
    "boolean": (pl.Boolean(), st.booleans()),
    "text": (pl.String(), text),
    "date": (pl.Date(), st.dates()),
    "timestamp": (pl.Datetime("us"), st.datetimes()),
    "timestamptz": (pl.Datetime("us", "UTC"), st.datetimes(timezones=st.just(UTC))),
    "time": (pl.Time(), st.times()),
}


@st.composite
def frames(draw: st.DrawFn) -> pl.DataFrame:
    kinds = draw(st.lists(st.sampled_from(sorted(COLUMN_KINDS)), min_size=1, max_size=5))
    height = draw(st.integers(0, 12))
    series = []
    for index, kind in enumerate(kinds):
        dtype, values = COLUMN_KINDS[kind]
        column = draw(st.lists(st.none() | values, min_size=height, max_size=height))
        series.append(pl.Series(f"c{index}_{kind}", column, dtype=dtype))
    return pl.DataFrame(series)


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(frames())
def test_any_frame_reads_back_exactly_even_when_loaded_twice(
    harness: Harness, frame: pl.DataFrame
) -> None:
    table = _new_table()
    try:
        for _ in range(2):
            loaded = _replace(harness, table, frame)
            # Rebuilt in the frame's own types: Postgres returns a real as its shortest
            # decimal text, which is exact as float32 but not as a Python float.
            stored = pl.DataFrame(
                harness.read_rows(table, frame.columns), schema=frame.schema, orient="row"
            )

            assert loaded == frame.height
            assert _comparable(stored.rows()) == _comparable(frame.rows())
    finally:
        harness.drop_table(table)
