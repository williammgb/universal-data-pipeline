import math
import uuid
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
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

from udp.errors import LoadError, SchemaDriftError
from udp.pipeline.load import with_platform_columns
from udp.quality.quarantine import quarantine
from udp.settings import Settings
from udp.storage.loader import (
    INTERRUPTED,
    CheckResult,
    DatasetState,
    Loader,
    LoadResult,
    RunFailure,
    RunFindings,
    RunStart,
    column_changes,
    column_type,
)
from udp.storage.postgres import PostgresLoader


@dataclass
class Harness:
    loader: Loader
    read_rows: Callable[[str, list[str]], list[tuple[Any, ...]]]
    read_columns: Callable[[str], list[str]]
    read_run: Callable[[UUID], dict[str, Any]]
    drop_table: Callable[[str], None]
    read_findings: Callable[[UUID], tuple[list[dict[str, Any]], list[dict[str, Any]]]]


def _memory_harness() -> Iterator[Harness]:
    loader = MemoryLoader()

    def drop_table(table: str) -> None:
        loader.tables.pop(table, None)

    def read_findings(run_id: UUID) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        return (
            [row for row in loader.quarantine if row["run_id"] == run_id],
            [row for row in loader.quality_results if row["run_id"] == run_id],
        )

    yield Harness(
        loader=loader,
        read_rows=lambda table, columns: loader.tables[table].select(columns).rows(),
        read_columns=lambda table: loader.tables[table].columns,
        read_run=lambda run_id: loader.runs[run_id],
        drop_table=drop_table,
        read_findings=read_findings,
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

        def read_columns(table: str) -> list[str]:
            rows = reader.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'datasets' AND table_name = %s ORDER BY ordinal_position",
                [table],
            ).fetchall()
            return [row[0] for row in rows]

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

        def read_findings(run_id: UUID) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
            with reader.cursor(row_factory=dict_row) as cursor:
                quarantine = cursor.execute(
                    "SELECT * FROM platform.quarantine WHERE run_id = %s ORDER BY quarantine_id",
                    [run_id],
                ).fetchall()
                results = cursor.execute(
                    "SELECT * FROM platform.quality_results WHERE run_id = %s ORDER BY position",
                    [run_id],
                ).fetchall()
            return quarantine, results

        yield Harness(loader, read_rows, read_columns, read_run, drop_table, read_findings)


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
        if isinstance(value, float) and math.isnan(value):
            return "NaN"
        return str(value) if isinstance(value, UUID) else value

    return Counter(tuple(normalise(value) for value in row) for row in rows)


def _replace(harness: Harness, table: str, *frames: pl.DataFrame) -> LoadResult:
    with harness.loader.transaction() as transaction:
        return transaction.replace_table(table, iter(frames))


def _append(harness: Harness, table: str, *frames: pl.DataFrame) -> LoadResult:
    with harness.loader.transaction() as transaction:
        return transaction.append_rows(table, iter(frames))


FIRST = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", None]})
SECOND = pl.DataFrame({"id": [9, 8], "name": ["x", ""]})


def test_replace_loads_every_row(harness: Harness) -> None:
    table = _new_table()

    loaded = _replace(harness, table, FIRST.head(2), FIRST.tail(1))

    assert loaded.rows == 3
    assert _comparable(harness.read_rows(table, FIRST.columns)) == _comparable(FIRST.rows())
    harness.drop_table(table)


def test_second_replace_leaves_only_the_second_rows(harness: Harness) -> None:
    table = _new_table()
    _replace(harness, table, FIRST)

    loaded = _replace(harness, table, SECOND)

    assert loaded.rows == 2
    assert _comparable(harness.read_rows(table, SECOND.columns)) == _comparable(SECOND.rows())
    harness.drop_table(table)


def test_append_adds_exactly_the_rows(harness: Harness) -> None:
    table = _new_table()
    _append(harness, table, FIRST)

    loaded = _append(harness, table, SECOND)

    assert loaded.rows == 2
    expected = _comparable(FIRST.rows() + SECOND.rows())
    assert _comparable(harness.read_rows(table, FIRST.columns)) == expected
    harness.drop_table(table)


def test_new_column_is_added_at_the_end_with_earlier_rows_empty(harness: Harness) -> None:
    table = _new_table()
    _append(harness, table, FIRST)

    loaded = _append(harness, table, SECOND.with_columns(pl.lit(True).alias("vip")))

    assert loaded.added == (("vip", "boolean"),)
    assert harness.read_columns(table) == ["id", "name", "vip"]
    rows = harness.read_rows(table, ["id", "vip"])
    assert _comparable(rows) == _comparable([(1, None), (2, None), (3, None), (9, True), (8, True)])
    harness.drop_table(table)


def test_column_missing_from_the_source_is_kept_and_empty(harness: Harness) -> None:
    table = _new_table()
    _append(harness, table, FIRST)

    loaded = _append(harness, table, SECOND.select("id"))

    assert loaded.missing == ("name",)
    assert harness.read_columns(table) == ["id", "name"]
    rows = harness.read_rows(table, ["id", "name"])
    assert _comparable(rows) == _comparable([*FIRST.rows(), (9, None), (8, None)])
    harness.drop_table(table)


def test_column_type_change_fails_and_keeps_the_table(harness: Harness) -> None:
    table = _new_table()
    _replace(harness, table, FIRST)

    with pytest.raises(SchemaDriftError, match=r"column 'id'.*bigint.*text"):
        _replace(harness, table, FIRST.with_columns(pl.col("id").cast(pl.String)))

    assert harness.read_columns(table) == ["id", "name"]
    assert _comparable(harness.read_rows(table, FIRST.columns)) == _comparable(FIRST.rows())
    harness.drop_table(table)


def test_dropped_table_is_created_fresh_with_the_new_types(harness: Harness) -> None:
    table = _new_table()
    _replace(harness, table, FIRST)
    as_text = FIRST.with_columns(pl.col("id").cast(pl.String))

    with harness.loader.transaction() as transaction:
        transaction.drop_table(table)
        transaction.replace_table(table, iter([as_text]))

    assert _comparable(harness.read_rows(table, ["id"])) == _comparable([("1",), ("2",), ("3",)])
    harness.drop_table(table)


def test_schema_versions_are_recorded_only_when_columns_change(harness: Harness) -> None:
    table = _new_table()
    run_id = _start_run(harness.loader)
    dataset = table.removeprefix("contract__")
    recorded = []
    for frame in (FIRST, SECOND, SECOND.with_columns(pl.lit(1).alias("extra"))):
        with harness.loader.transaction() as transaction:
            transaction.append_rows(table, iter([frame]))
            now = datetime.now(UTC)
            recorded.append(transaction.record_columns(table, "contract", dataset, run_id, now))

    assert recorded == [1, None, 2]
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
            succeeded, ended_at=datetime.now(UTC), rows_extracted=3, rows_loaded=loaded.rows
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


LOCK_STEPS = st.lists(st.tuples(st.booleans(), st.sampled_from(["d0", "d1", "d2"])), max_size=12)


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(LOCK_STEPS)
def test_a_dataset_lock_is_taken_exactly_when_nobody_holds_it(
    harness: Harness, steps: list[tuple[bool, str]]
) -> None:
    source = f"lock{uuid.uuid4().hex[:12]}"
    held: set[str] = set()

    for lock, dataset in steps:
        if lock:
            assert harness.loader.lock_dataset(source, dataset) == (dataset not in held)
            held.add(dataset)
        else:
            harness.loader.unlock_dataset(source, dataset)
            held.discard(dataset)
    for dataset in held:
        harness.loader.unlock_dataset(source, dataset)


RUN_STATUSES = ["running", "succeeded", "failed", "skipped"]


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    st.lists(
        st.tuples(
            st.sampled_from(["this", "other source", "other dataset"]),
            st.sampled_from(RUN_STATUSES),
        ),
        max_size=8,
    )
)
def test_only_the_datasets_running_runs_become_interrupted(
    harness: Harness, runs: list[tuple[str, str]]
) -> None:
    source, other = f"runs{uuid.uuid4().hex[:12]}", f"other{uuid.uuid4().hex[:12]}"
    places = {"this": (source, "t"), "other source": (other, "t"), "other dataset": (source, "u")}
    loader = harness.loader
    created = []
    for place, status in runs:
        run = RunStart(uuid.uuid7(), *places[place], "manual", datetime.now(UTC))
        if status == "skipped":
            loader.skip_run(run, ended_at=datetime.now(UTC))
        else:
            loader.start_run(run)
        if status == "succeeded":
            with loader.transaction() as transaction:
                transaction.succeed_run(
                    run.run_id, ended_at=datetime.now(UTC), rows_extracted=0, rows_loaded=0
                )
        if status == "failed":
            failure = RunFailure("ExtractError", "file not found", "Traceback ...")
            loader.fail_run(
                run.run_id, ended_at=datetime.now(UTC), rows_extracted=0, failure=failure
            )
        created.append((run.run_id, place, status))
    found_by = uuid.uuid7()

    count = loader.fail_interrupted_runs(source, "t", found_by=found_by, ended_at=datetime.now(UTC))

    interrupted = [(p, s) for _, p, s in created].count(("this", "running"))
    assert count == interrupted
    for run_id, place, status in created:
        record = harness.read_run(run_id)
        if (place, status) == ("this", "running"):
            assert (record["status"], record["error_class"]) == ("failed", INTERRUPTED)
            assert str(found_by) in record["error_message"]
            assert record["ended_at"] is not None
        else:
            assert record["status"] == status
            assert record["error_class"] != INTERRUPTED


@pytest.mark.db
def test_a_postgres_lock_holds_across_connections_until_its_holder_closes() -> None:
    url = Settings().database_url  # type: ignore[call-arg]
    source = f"lock{uuid.uuid4().hex[:12]}"
    with PostgresLoader(url) as waiting:
        with PostgresLoader(url) as holder:
            assert holder.lock_dataset(source, "t")
            assert not waiting.lock_dataset(source, "t")
        assert waiting.lock_dataset(source, "t")
        waiting.unlock_dataset(source, "t")


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

            assert loaded.rows == frame.height
            assert _comparable(stored.rows()) == _comparable(frame.rows())
    finally:
        harness.drop_table(table)


NAMES = st.sampled_from(["a", "b", "c", "d"])
STORED_TYPES = st.sampled_from(["bigint", "date", "text", "boolean"])
INCOMING_TYPES = st.sampled_from([pl.Int64(), pl.Date(), pl.String(), pl.Boolean(), pl.Null()])


@given(
    st.dictionaries(NAMES, STORED_TYPES, max_size=4),
    st.dictionaries(NAMES, INCOMING_TYPES, max_size=4),
)
def test_columns_are_only_added_and_types_never_change(
    existing: dict[str, str], incoming: dict[str, pl.DataType]
) -> None:
    schema = pl.Schema(incoming)
    conflicts = [
        name
        for name, dtype in incoming.items()
        if name in existing
        and not isinstance(dtype, pl.Null)
        and column_type(name, dtype) != existing[name]
    ]

    if conflicts:
        with pytest.raises(SchemaDriftError, match=f"column '{conflicts[0]}'"):
            column_changes("t", list(existing.items()), schema)
        return
    changes = column_changes("t", list(existing.items()), schema)

    assert [name for name, _ in changes.added] == [n for n in incoming if n not in existing]
    assert list(changes.missing) == [name for name in existing if name not in incoming]
    widened = [*existing, *(name for name, _ in changes.added)]
    assert widened[: len(existing)] == list(existing)


@st.composite
def merge_batches(draw: st.DrawFn) -> tuple[list[str], list[pl.DataFrame]]:
    key = ["k1", "k2"] if draw(st.booleans()) else ["k1"]
    batches: list[pl.DataFrame] = []
    for _ in range(draw(st.integers(1, 4))):
        if batches and draw(st.integers(0, 4)) == 0:
            batches.append(batches[-1])
            continue
        height = draw(st.integers(0, 8))
        data: dict[str, pl.Series] = {
            "k1": pl.Series(
                draw(st.lists(st.integers(1, 4), min_size=height, max_size=height)), dtype=pl.Int64
            ),
            "wm": pl.Series(
                draw(st.lists(st.integers(0, 3), min_size=height, max_size=height)), dtype=pl.Int64
            ),
            "v1": pl.Series(
                draw(
                    st.lists(
                        st.none() | st.sampled_from(["x", "y"]), min_size=height, max_size=height
                    )
                ),
                dtype=pl.String,
            ),
        }
        if "k2" in key:
            data["k2"] = pl.Series(
                draw(st.lists(st.sampled_from(["p", "q"]), min_size=height, max_size=height)),
                dtype=pl.String,
            )
        if draw(st.booleans()):
            data["v2"] = pl.Series(
                draw(st.lists(st.none() | st.integers(0, 2), min_size=height, max_size=height)),
                dtype=pl.Int64,
            )
        batches.append(pl.DataFrame(data))
    return key, batches


def _model_merge(
    stored: dict[tuple[Any, ...], dict[str, Any]], batch: pl.DataFrame, key: list[str]
) -> int:
    latest: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in batch.to_dicts():
        row_key = tuple(row[name] for name in key)
        if row_key not in latest or row["wm"] >= latest[row_key]["wm"]:
            latest[row_key] = row
    written = 0
    for row_key, row in latest.items():
        if row_key not in stored:
            stored[row_key] = row
            written += 1
        elif stored[row_key]["_record_hash"] != row["_record_hash"]:
            stored[row_key] = {**stored[row_key], **row}
            written += 1
    return written


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(merge_batches())
def test_merge_matches_a_simple_model(
    harness: Harness, generated: tuple[list[str], list[pl.DataFrame]]
) -> None:
    key, batches = generated
    table = _new_table()
    model: dict[tuple[Any, ...], dict[str, Any]] = {}
    try:
        prepared = pl.DataFrame()
        for batch in batches:
            prepared = with_platform_columns(batch, uuid.uuid7(), datetime.now(UTC))
            expected = _model_merge(model, prepared, key)
            with harness.loader.transaction() as transaction:
                result = transaction.merge_rows(
                    table, iter([prepared]), primary_key=key, watermark="wm"
                )
            assert result.rows == expected

        columns = harness.read_columns(table)
        stored_rows = _comparable(harness.read_rows(table, columns))
        model_rows = _comparable(
            [tuple(row.get(name) for name in columns) for row in model.values()]
        )
        assert stored_rows == model_rows

        with harness.loader.transaction() as transaction:
            again = transaction.merge_rows(table, iter([prepared]), primary_key=key, watermark="wm")
        assert again.rows == 0
        assert _comparable(harness.read_rows(table, columns)) == stored_rows
    finally:
        harness.drop_table(table)


WATERMARKS = st.one_of(
    st.tuples(st.just("bigint"), st.integers(-(2**63), 2**63 - 1)),
    st.tuples(st.just("date"), st.dates()),
    st.tuples(st.just("timestamp without time zone"), st.datetimes()),
    st.tuples(st.just("timestamp with time zone"), st.datetimes(timezones=st.just(UTC))),
    st.tuples(st.none(), st.none()),
)


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(WATERMARKS, st.booleans())
def test_saved_state_reads_back_equal(
    harness: Harness, watermark: tuple[str | None, Any], with_file: bool
) -> None:
    kind, value = watermark
    state = DatasetState(
        source="contract_state",
        dataset=f"d{uuid.uuid4().hex[:12]}",
        load_mode="merge" if kind else "full",
        primary_key=("id", "line") if kind else (),
        watermark_column="updated_at" if kind else None,
        watermark_type=kind,
        watermark=value,
        file_path="data/orders.csv" if with_file else None,
        file_sha256="ab" * 32 if with_file else None,
        config_sha256="cd" * 32,
        run_id=uuid.uuid7(),
        saved_at=datetime.now(UTC),
    )

    with harness.loader.transaction() as transaction:
        transaction.save_state(state)
    with harness.loader.transaction() as transaction:
        assert transaction.read_state(state.source, state.dataset) == state
        assert transaction.read_state(state.source, "never_loaded") is None


# --- slice 5: decimals, table checks and findings -------------------------------------------


def test_decimals_are_stored_exactly_and_merging_them_again_writes_nothing(
    harness: Harness,
) -> None:
    table = _new_table()
    frame = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "wm": [1, 1, 1],
            "amount": [Decimal("12.30"), Decimal("-0.01"), Decimal("9999999999.99")],
        },
        schema={"id": pl.Int64, "wm": pl.Int64, "amount": pl.Decimal(12, 2)},
    )
    prepared = with_platform_columns(frame, uuid.uuid7(), datetime.now(UTC))
    assert column_type("amount", pl.Decimal(12, 2)) == "numeric(12,2)"
    try:
        # The second merge compares the stored column type with numeric(12,2), so a spelling
        # that differs from Postgres' own would fail it as a type change.
        for expected in (3, 0):
            with harness.loader.transaction() as transaction:
                result = transaction.merge_rows(
                    table, iter([prepared]), primary_key=["id"], watermark="wm"
                )
            assert result.rows == expected
        assert sorted(harness.read_rows(table, ["id", "amount"])) == [
            (1, Decimal("12.30")),
            (2, Decimal("-0.01")),
            (3, Decimal("9999999999.99")),
        ]
    finally:
        harness.drop_table(table)


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    st.integers(1, 3).flatmap(
        lambda width: st.lists(
            st.tuples(*[st.one_of(st.none(), st.integers(0, 2)) for _ in range(width)]),
            min_size=1,
            max_size=12,
        )
    ),
    st.integers(0, 12),
)
def test_duplicate_rows_count_rows_sharing_non_null_keys(
    harness: Harness, rows: list[tuple[int | None, ...]], split: int
) -> None:
    columns = [f"k{index}" for index in range(len(rows[0]))]
    frame = pl.DataFrame(rows, schema=dict.fromkeys(columns, pl.Int64), orient="row")
    table = _new_table()
    counts = Counter(row for row in rows if None not in row)
    expected = sum(count for count in counts.values() if count > 1)
    try:
        for part in (frame[:split], frame[split:]):
            if part.height:
                _append(harness, table, part)
        with harness.loader.transaction() as transaction:
            assert transaction.duplicate_rows(table, columns) == expected
            assert transaction.table_rows(table) == frame.height
    finally:
        harness.drop_table(table)


def test_newest_value_reads_dates_and_both_kinds_of_timestamp(harness: Harness) -> None:
    table = _new_table()
    frame = pl.DataFrame(
        {
            "day": [date(2024, 1, 2), date(2024, 1, 1), None],
            "naive": [datetime(2024, 1, 1, 10), datetime(2024, 1, 1, 11), None],
            "utc": [datetime(2024, 1, 1, 10, tzinfo=UTC), None, None],
            "empty": pl.Series([None, None, None], dtype=pl.Date),
        }
    )
    try:
        _replace(harness, table, frame)
        with harness.loader.transaction() as transaction:
            assert transaction.newest_value(table, "day") == date(2024, 1, 2)
            assert transaction.newest_value(table, "naive") == datetime(2024, 1, 1, 11)
            assert transaction.newest_value(table, "utc") == datetime(2024, 1, 1, 10, tzinfo=UTC)
            assert transaction.newest_value(table, "empty") is None
    finally:
        harness.drop_table(table)


def _findings(dataset: str, table_rows: int | None) -> RunFindings:
    findings = RunFindings("contract_quality", dataset)
    quarantine(
        findings,
        pl.DataFrame({"id": [7], "amount": [1.5], "day": [date(2024, 1, 2)]}),
        pl.Series(["check 0 range failed on column 'amount'"]),
    )
    findings.results.append(
        CheckResult(
            position=0,
            check_type="min_rows",
            columns=(),
            severity="warn",
            passed=False,
            failing_rows=None,
            table_rows=table_rows,
            message="the table has 3 rows, at least 5 required",
            settings={"rows": 5},
        )
    )
    return findings


def test_findings_are_recorded_and_only_succeeded_runs_give_a_previous_count(
    harness: Harness,
) -> None:
    dataset = f"d{uuid.uuid4().hex[:12]}"
    succeeded, failed = _start_run(harness.loader), _start_run(harness.loader)
    now = datetime.now(UTC)
    with harness.loader.transaction() as transaction:
        assert transaction.previous_table_rows("contract_quality", dataset) is None
        transaction.record_findings(succeeded, _findings(dataset, 3), now)
        transaction.succeed_run(succeeded, ended_at=now, rows_extracted=3, rows_loaded=3)
    with harness.loader.transaction() as transaction:
        transaction.record_findings(failed, _findings(dataset, 9), now)
    harness.loader.fail_run(
        failed, ended_at=now, rows_extracted=9, failure=RunFailure("QualityError", "x", "")
    )

    quarantined, results = harness.read_findings(succeeded)
    assert [(row["reason"], row["record"]) for row in quarantined] == [
        ("check 0 range failed on column 'amount'", {"id": 7, "amount": "1.5", "day": "2024-01-02"})
    ]
    assert [
        (r["position"], r["check_type"], r["severity"], r["passed"], r["table_rows"], r["settings"])
        for r in results
    ] == [(0, "min_rows", "warn", False, 3, {"rows": 5})]
    assert harness.read_run(succeeded)["rows_quarantined"] == 1
    assert harness.read_run(failed)["rows_quarantined"] == 1
    with harness.loader.transaction() as transaction:
        assert transaction.previous_table_rows("contract_quality", dataset) == 3
