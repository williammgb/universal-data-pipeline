"""The constraint engine: every constraint type against a fixture whose violations are known, the
data left untouched, counts per constraint, and results stored against a stage and a run."""

import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime
from typing import Any
from uuid import UUID

import polars as pl
import psycopg
import pytest
from hypothesis import given
from hypothesis import strategies as st
from polars.testing import assert_frame_equal
from psycopg import sql
from pydantic import TypeAdapter

from udp.config.constraints import Constraint
from udp.connectors.csv import CsvDataset
from udp.errors import LoadError, ValidationError
from udp.names import Stage, stage_table
from udp.pipeline.load import with_platform_columns
from udp.quality.constraints import MAX_VIOLATIONS, check_frame
from udp.settings import Settings
from udp.storage.loader import RunRef, RunStart
from udp.storage.postgres import PostgresLoader

NOW = datetime(2026, 10, 4, 12, tzinfo=UTC)
CONSTRAINTS = TypeAdapter(list[Constraint])

# Every column breaks the constraint of the same name below a known number of times.
KNOWN = pl.DataFrame(
    {
        "id": [1, 2, 3, 4, 5, 6],
        "email": ["a@x.io", "b@x.io", None, "a@x.io", "e@x.io", "f@x.io"],
        "amount": [10.0, -5.0, 20.0, 1500.0, 30.0, None],
        "status": ["open", "closed", "open", "lost", "open", "gone"],
        "zip": ["1234AB", "12345", "9999ZZ", "x", "1111AA", None],
        "qty": ["1", "2", "three", "4", "5.5", "6"],
    }
)


def _constraints(*definitions: dict[str, Any]) -> list[Constraint]:
    return CONSTRAINTS.validate_python(list(definitions))


@pytest.mark.parametrize(
    ("definition", "ids", "values"),
    [
        ({"constraint": "datatype", "column": "qty", "type": "integer"}, [3, 5], ["three", "5.5"]),
        ({"constraint": "not_null", "column": "email"}, [3], [None]),
        ({"constraint": "min", "column": "amount", "value": 0}, [2], ["-5.0"]),
        ({"constraint": "max", "column": "amount", "value": 1000}, [4], ["1500.0"]),
        ({"constraint": "unique", "columns": ["email"]}, [1, 4], ["a@x.io", "a@x.io"]),
        (
            {"constraint": "allowed_values", "column": "status", "values": ["open", "closed"]},
            [4, 6],
            ["lost", "gone"],
        ),
        (
            {"constraint": "pattern", "column": "zip", "pattern": "[0-9]{4}[A-Z]{2}"},
            [2, 4],
            ["12345", "x"],
        ),
    ],
)
def test_each_constraint_type_counts_its_known_violations(
    definition: dict[str, Any], ids: list[int], values: list[str | None]
) -> None:
    (outcome,) = check_frame(KNOWN, _constraints(definition), primary_key=["id"])

    assert outcome.constraint.constraint == definition["constraint"]
    assert not outcome.passed
    assert (outcome.failing_rows, outcome.failing_values) == (len(ids), len(ids))
    assert [v.row_key for v in outcome.violations] == [{"id": i} for i in ids]
    assert [v.value for v in outcome.violations] == values
    shown = "1 row and 1 value" if len(ids) == 1 else f"{len(ids)} rows and {len(ids)} values"
    assert outcome.message == f"{shown} break it"


def test_a_constraint_every_row_holds_passes() -> None:
    (outcome,) = check_frame(KNOWN, _constraints({"constraint": "unique", "columns": ["id"]}))

    assert outcome.passed
    assert (outcome.failing_rows, outcome.failing_values, outcome.violations) == (0, 0, ())
    assert outcome.message == "every row holds"


def test_a_failing_constraint_leaves_the_data_untouched() -> None:
    before = KNOWN.clone()
    constraints = _constraints(
        {"constraint": "min", "column": "amount", "value": 0, "critical": True}
    )

    (outcome,) = check_frame(KNOWN, constraints, primary_key=["id"])

    assert_frame_equal(KNOWN, before)
    assert KNOWN.height == 6
    result = outcome.result("shop", "orders", Stage.RAW, RunRef(ingest_run_id=uuid.uuid7()), NOW)
    assert (result.constraint_type, result.columns, result.critical) == ("min", ("amount",), True)
    assert result.settings == {"column": "amount", "value": 0}
    (violation,) = result.violations
    assert (violation.column, violation.row_key, violation.value) == ("amount", {"id": 2}, "-5.0")


def test_counts_are_per_constraint_when_one_value_breaks_two() -> None:
    frame = pl.DataFrame({"code": ["ab", "AB", "abc", "A1"]})
    constraints = _constraints(
        {"constraint": "allowed_values", "column": "code", "values": ["ab", "abc"]},
        {"constraint": "pattern", "column": "code", "pattern": "[a-z]{2}"},
    )

    allowed, pattern = check_frame(frame, constraints)

    # "AB" and "A1" break both; "abc" breaks only the pattern.
    assert (allowed.position, allowed.failing_rows) == (1, 2)
    assert [v.value for v in allowed.violations] == ["AB", "A1"]
    assert (pattern.position, pattern.failing_rows) == (2, 3)
    assert [v.value for v in pattern.violations] == ["AB", "abc", "A1"]


def test_unique_over_two_columns_counts_rows_and_values() -> None:
    frame = pl.DataFrame({"a": [1, 1, 1, 2, None, None], "b": ["x", "x", "y", "x", "z", "z"]})

    (outcome,) = check_frame(frame, _constraints({"constraint": "unique", "columns": ["a", "b"]}))

    # Rows with a missing value in the key are never duplicates, as in V1's unique check.
    assert (outcome.failing_rows, outcome.failing_values) == (2, 4)
    assert outcome.message == "2 rows and 4 values break it"
    assert [(v.column, v.row_key, v.value) for v in outcome.violations] == [
        ("a", {"row": 1}, "1"),
        ("b", {"row": 1}, "x"),
        ("a", {"row": 2}, "1"),
        ("b", {"row": 2}, "x"),
    ]


def test_missing_values_break_only_not_null() -> None:
    frame = pl.DataFrame({"n": [None, None], "s": [None, "ok"]}, schema={"n": pl.Int64, "s": str})
    constraints = _constraints(
        {"constraint": "datatype", "column": "s", "type": "integer"},
        {"constraint": "min", "column": "n", "value": 1},
        {"constraint": "allowed_values", "column": "s", "values": ["ok"]},
        {"constraint": "pattern", "column": "s", "pattern": "ok"},
        {"constraint": "unique", "columns": ["n"]},
        {"constraint": "not_null", "column": "n"},
    )

    outcomes = check_frame(frame, constraints)

    assert [o.failing_rows for o in outcomes] == [1, 0, 0, 0, 0, 2]


def test_a_broken_list_or_struct_is_recorded_as_json() -> None:
    frame = pl.DataFrame({"tags": [["a", "b"], None], "s": [{"d": date(2024, 1, 1), "n": 1}, None]})
    constraints = _constraints(
        {"constraint": "datatype", "column": "tags", "type": "integer"},
        {"constraint": "datatype", "column": "s", "type": "integer"},
    )

    tags, struct = check_frame(frame, constraints)

    assert [v.value for v in tags.violations] == ['["a", "b"]']
    assert [v.value for v in struct.violations] == ['{"d": "2024-01-01", "n": 1}']


def test_a_row_is_named_by_its_record_hash_without_a_primary_key() -> None:
    frame = pl.DataFrame({"v": [1, -1], "_record_hash": ["h1", "h2"]})

    (outcome,) = check_frame(frame, _constraints({"constraint": "min", "column": "v", "value": 0}))

    assert [v.row_key for v in outcome.violations] == [{"_record_hash": "h2"}]


def test_violation_records_stop_at_the_cap_while_the_counts_stay_exact() -> None:
    frame = pl.DataFrame({"v": list(range(-250, 0))})

    (outcome,) = check_frame(frame, _constraints({"constraint": "min", "column": "v", "value": 0}))

    assert outcome.failing_rows == 250
    assert len(outcome.violations) == MAX_VIOLATIONS


def test_a_constraint_on_a_column_not_in_the_data_is_refused() -> None:
    constraints = _constraints(
        {"constraint": "not_null", "column": "id"},
        {"constraint": "min", "column": "price", "value": 0},
    )

    with pytest.raises(ValidationError, match=r"constraint 2 \(min\): column 'price' is not"):
        check_frame(KNOWN, constraints)


def test_min_on_a_text_column_is_refused_with_the_hint_to_declare_its_type() -> None:
    constraints = _constraints({"constraint": "min", "column": "qty", "value": 0})

    with pytest.raises(ValidationError, match=r"constraint 1 \(min\).*declare its type"):
        check_frame(KNOWN, constraints)


def test_no_constraints_give_no_outcomes() -> None:
    assert check_frame(KNOWN, []) == []


@given(
    values=st.lists(st.one_of(st.none(), st.integers(-50, 50)), max_size=300),
    bound=st.integers(-60, 60),
)
def test_min_counts_exactly_the_values_below_its_bound(
    values: list[int | None], bound: int
) -> None:
    frame = pl.DataFrame({"v": values}, schema={"v": pl.Int64})
    below = [v for v in values if v is not None and v < bound]

    (outcome,) = check_frame(
        frame, _constraints({"constraint": "min", "column": "v", "value": bound})
    )

    assert (outcome.failing_rows, outcome.failing_values) == (len(below), len(below))
    assert [v.value for v in outcome.violations] == [str(v) for v in below[:MAX_VIOLATIONS]]
    assert frame["v"].to_list() == values


# --- stage tables in PostgreSQL ----------------------------------------------------------------


@pytest.fixture
def loader() -> Iterator[PostgresLoader]:
    with PostgresLoader(Settings().database_url) as made:  # type: ignore[call-arg]
        yield made


@pytest.fixture
def source(loader: PostgresLoader) -> Iterator[str]:
    """A throwaway source name; its stage tables are dropped afterwards."""
    name = f"con{uuid.uuid4().hex[:10]}"
    yield name
    with psycopg.connect(Settings().database_url, autocommit=True) as conn:  # type: ignore[call-arg]
        for stage in Stage:
            conn.execute(
                sql.SQL("DROP TABLE IF EXISTS {}").format(
                    sql.Identifier(*stage_table(stage, name, "orders"))
                )
            )


def _ingest(loader: PostgresLoader, source: str, frame: pl.DataFrame) -> UUID:
    run = RunStart(uuid.uuid7(), source, "orders", "manual", NOW)
    loader.start_run(run)
    with loader.stages() as stages:
        stages.append_raw(source, "orders", [with_platform_columns(frame, run.run_id, NOW)])
    with loader.transaction() as transaction:
        transaction.succeed_run(
            run.run_id, ended_at=NOW, rows_extracted=frame.height, rows_loaded=frame.height
        )
    return run.run_id


def _raw_rows(source: str) -> list[tuple[Any, ...]]:
    with psycopg.connect(Settings().database_url) as conn:  # type: ignore[call-arg]
        return conn.execute(
            sql.SQL("SELECT * FROM {} ORDER BY id, _run_id").format(
                sql.Identifier(*stage_table(Stage.RAW, source, "orders"))
            )
        ).fetchall()


STAGE_CONSTRAINTS: list[dict[str, Any]] = [
    {"constraint": "datatype", "column": "qty", "type": "integer"},
    {"constraint": "not_null", "column": "email", "critical": True},
    {"constraint": "min", "column": "amount", "value": 0},
    {"constraint": "max", "column": "amount", "value": 1000},
    {"constraint": "unique", "columns": ["email"]},
    {"constraint": "allowed_values", "column": "status", "values": ["open", "closed"]},
    {"constraint": "pattern", "column": "zip", "pattern": "[0-9]{4}[A-Z]{2}"},
]


@pytest.mark.db
def test_constraints_on_a_stage_are_stored_and_read_back_by_run(
    loader: PostgresLoader, source: str
) -> None:
    dataset = CsvDataset.model_validate(
        {"name": "orders", "path": "orders.csv", "constraints": STAGE_CONSTRAINTS}
    )
    first = _ingest(loader, source, KNOWN.head(3))
    second = _ingest(loader, source, KNOWN.tail(3))
    before = _raw_rows(source)

    with loader.stages() as stages:
        stored = stages.check_constraints(
            Stage.RAW, source, dataset, RunRef(ingest_run_id=second), checked_at=NOW
        )
        stages.record_constraint_results(
            [
                outcome.result(source, "orders", Stage.RAW, RunRef(ingest_run_id=first), NOW)
                for outcome in check_frame(KNOWN.head(3), dataset.constraints)
            ]
        )
    assert _raw_rows(source) == before

    with loader.stages() as stages:
        read = stages.read_constraint_results(
            source, "orders", Stage.RAW, RunRef(ingest_run_id=second)
        )
        everything = stages.read_constraint_results(source, "orders", Stage.RAW)
        none_in_clean = stages.read_constraint_results(
            source, "orders", Stage.CLEAN, RunRef(ingest_run_id=second)
        )

    assert len(everything) == 14
    assert none_in_clean == []
    # The whole table is checked, both runs' rows: the counts the frame engine gives on KNOWN.
    expected = check_frame(KNOWN, dataset.constraints)
    assert [(r.position, r.constraint_type) for r in read] == [
        (position, c["constraint"]) for position, c in enumerate(STAGE_CONSTRAINTS, 1)
    ]
    assert [(r.failing_rows, r.failing_values) for r in read] == [
        (o.failing_rows, o.failing_values) for o in expected
    ]
    assert [r.critical for r in read] == [False, True, *[False] * 5]
    assert {r.run for r in read} == {RunRef(ingest_run_id=second)}
    assert [(r.checked_at, r.violations) for r in read] == [
        (r.checked_at, r.violations) for r in stored
    ]
    unique = read[4]
    assert sorted(v.value or "" for v in unique.violations) == ["a@x.io", "a@x.io"]
    assert all(set(v.row_key) == {"_record_hash"} for r in read for v in r.violations)


@pytest.mark.db
def test_without_a_key_every_constraint_names_a_row_by_the_same_position(
    loader: PostgresLoader, source: str
) -> None:
    raw = sql.Identifier(*stage_table(Stage.RAW, source, "orders"))
    with psycopg.connect(Settings().database_url, autocommit=True) as conn:  # type: ignore[call-arg]
        conn.execute(sql.SQL("CREATE TABLE {} (email text)").format(raw))
        conn.execute(
            sql.SQL("INSERT INTO {} VALUES ('a@x.io'), (NULL), ('a@x.io'), ('b@x.io')").format(raw)
        )
    dataset = CsvDataset.model_validate(
        {
            "name": "orders",
            "path": "orders.csv",
            "constraints": [
                {"constraint": "not_null", "column": "email"},
                {"constraint": "unique", "columns": ["email"]},
            ],
        }
    )

    run = RunStart(uuid.uuid7(), source, "orders", "manual", NOW)
    loader.start_run(run)

    with loader.stages() as stages:
        missing, unique = stages.check_constraints(
            Stage.RAW, source, dataset, RunRef(ingest_run_id=run.run_id), checked_at=NOW
        )

    assert [v.row_key for v in missing.violations] == [{"row": 2}]
    assert [v.row_key for v in unique.violations] == [{"row": 1}, {"row": 3}]


@pytest.mark.db
def test_constraints_on_a_stage_with_no_table_are_refused(
    loader: PostgresLoader, source: str
) -> None:
    dataset = CsvDataset.model_validate(
        {"name": "orders", "path": "orders.csv", "constraints": STAGE_CONSTRAINTS}
    )

    with loader.stages() as stages, pytest.raises(LoadError, match="has no clean table"):
        stages.check_constraints(
            Stage.CLEAN, source, dataset, RunRef(ingest_run_id=uuid.uuid7()), checked_at=NOW
        )
