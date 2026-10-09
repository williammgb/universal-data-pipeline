"""The transformation framework and the standard transformations, each over a frame with known
values."""

from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import polars as pl
import pytest
from hypothesis import given
from hypothesis import strategies as st
from polars.testing import assert_frame_equal, assert_series_equal
from pydantic import TypeAdapter

from udp.config.constraints import Constraint
from udp.errors import ConfigError
from udp.transformations import (
    TRANSFORMATIONS,
    StepContext,
    StepResult,
    Transformation,
    load_steps,
    register,
    run_step,
)
from udp.transformations.base import Applied

PEOPLE = pl.DataFrame(
    {
        "name": [" Ann ", "bob", None, "ann", "CAROL"],
        "age": [30, None, 40, None, 25],
        "score": [1.0, 2.0, None, 4.0, 100.0],
    }
)


def _one(definition: dict[str, Any], frame: pl.DataFrame = PEOPLE) -> Transformation:
    [step] = load_steps([definition], frame.schema)
    return step


def _run(
    definition: dict[str, Any],
    frame: pl.DataFrame = PEOPLE,
    context: StepContext | None = None,
) -> tuple[pl.DataFrame, StepResult]:
    return run_step(frame, _one(definition, frame), 1, context)


def _constraints(*definitions: dict[str, Any]) -> list[Constraint]:
    return TypeAdapter(list[Constraint]).validate_python(list(definitions))


# --- missing values ---------------------------------------------------------------------------


def test_drop_missing_drops_rows_with_a_null_in_the_named_columns() -> None:
    frame, result = _run({"type": "drop_missing", "columns": ["age"]})
    assert frame["age"].to_list() == [30, 40, 25]
    assert (result.status, result.rows_in, result.rows_out) == ("succeeded", 5, 3)


def test_drop_missing_with_no_columns_drops_a_null_anywhere() -> None:
    frame, result = _run({"type": "drop_missing"})
    assert frame["name"].to_list() == [" Ann ", "CAROL"]
    assert result.rows_out == 2


@pytest.mark.parametrize(
    ("method", "value", "column", "filled"),
    [
        # age 30, 40, 25: the mean is 31.67, rounded to 32 so the column stays integer.
        ("mean", None, "age", 32),
        ("median", None, "age", 30),
        # score 1, 2, 4, 100: mean 26.75, median 3.0.
        ("mean", None, "score", 26.75),
        ("median", None, "score", 3.0),
        ("value", 0, "age", 0),
        ("value", "unknown", "name", "unknown"),
    ],
)
def test_fill_missing_writes_the_known_value_into_each_null(
    method: str, value: Any, column: str, filled: Any
) -> None:
    definition = {"type": "fill_missing", "columns": [column], "method": method}
    if value is not None:
        definition["value"] = value
    frame, result = _run(definition)
    nulls = PEOPLE[column].is_null()
    assert frame[column].filter(nulls).to_list() == [filled] * int(nulls.sum())
    assert_series_equal(frame[column].filter(~nulls), PEOPLE[column].filter(~nulls))
    assert frame[column].dtype == PEOPLE[column].dtype
    assert result.status == "succeeded"


def test_fill_with_the_mode_takes_the_most_common_value_and_the_smallest_on_a_tie() -> None:
    frame = pl.DataFrame({"city": ["Oslo", None, "Bergen", "Oslo", None], "n": [3, 1, 1, 3, None]})
    filled, _ = _run({"type": "fill_missing", "columns": ["city", "n"], "method": "mode"}, frame)
    assert filled["city"].to_list() == ["Oslo", "Oslo", "Bergen", "Oslo", "Oslo"]
    tie = pl.DataFrame({"n": [5, 2, None, 5, 2]})
    filled, _ = _run({"type": "fill_missing", "columns": ["n"], "method": "mode"}, tie)
    assert filled["n"].to_list() == [5, 2, 2, 5, 2]


def test_a_whole_number_fill_rounds_a_half_up() -> None:
    frame = pl.DataFrame({"n": [2, 3, None]})
    filled, _ = _run({"type": "fill_missing", "columns": ["n"], "method": "mean"}, frame)
    assert filled["n"].to_list() == [2, 3, 3]


def test_mean_uses_the_whole_frame_not_a_chunk_of_it() -> None:
    # A V1 load reads 100,000 rows at a time; the first chunk's mean here would be 0.
    values = [0.0] * 100_000 + [10.0] * 100_000 + [None]
    frame, _ = _run(
        {"type": "fill_missing", "columns": ["v"], "method": "mean"}, pl.DataFrame({"v": values})
    )
    assert frame["v"][-1] == 5.0


def test_median_fill_on_an_all_null_column_fails_and_leaves_nulls() -> None:
    empty = pl.DataFrame({"v": [None, None, None]}, schema={"v": pl.Float64})
    frame, result = _run({"type": "fill_missing", "columns": ["v"], "method": "median"}, empty)
    assert result.status == "failed"
    assert result.error is not None and "column 'v' has no values" in result.error
    assert frame["v"].null_count() == 3
    assert result.values_changed == 0


@given(st.lists(st.one_of(st.none(), st.integers(-1000, 1000)), min_size=1, max_size=50))
def test_a_fill_changes_exactly_the_nulls_and_a_second_run_changes_nothing(
    values: list[int | None],
) -> None:
    frame = pl.DataFrame({"v": values}, schema={"v": pl.Int64})
    step = _one({"type": "fill_missing", "columns": ["v"], "method": "median"}, frame)
    once, first = run_step(frame, step, 1)
    if frame["v"].null_count() == len(values):
        assert first.status == "failed"
        return
    assert first.values_changed == frame["v"].null_count()
    assert once["v"].null_count() == 0
    _, second = run_step(once, step, 1)
    assert second.values_changed == 0


# --- standardization --------------------------------------------------------------------------


def test_convert_type_converts_text_to_integers() -> None:
    frame = pl.DataFrame({"n": ["1", "2", None, "+3"]})
    converted, result = _run({"type": "convert_type", "column": "n", "to": "integer"}, frame)
    assert converted["n"].to_list() == [1, 2, None, 3]
    assert converted["n"].dtype == pl.Int64
    assert result.status == "succeeded"


def test_convert_type_fails_and_changes_nothing_when_a_value_does_not_fit() -> None:
    frame = pl.DataFrame({"n": ["1", "two", "3"]})
    converted, result = _run({"type": "convert_type", "column": "n", "to": "integer"}, frame)
    assert result.status == "failed"
    assert result.error is not None and "1 value in column 'n' do not fit integer" in result.error
    assert_frame_equal(converted, frame)


def test_normalize_column_names_cleans_every_name() -> None:
    frame = pl.DataFrame({"First Name": ["a"], "AGE": [1], "e-mail": ["x"]})
    renamed, result = _run({"type": "normalize_column_names"}, frame)
    assert renamed.columns == ["first_name", "age", "e_mail"]
    assert result.message == "3 column names changed"


def test_normalize_values_trims_then_changes_case_then_maps() -> None:
    frame, _ = _run(
        {
            "type": "normalize_values",
            "columns": ["name"],
            "trim": True,
            "case": "lower",
            "mapping": {"bob": "robert"},
        }
    )
    assert frame["name"].to_list() == ["ann", "robert", None, "ann", "carol"]


def test_normalize_values_title_case() -> None:
    frame, _ = _run({"type": "normalize_values", "columns": ["name"], "case": "title"})
    assert frame["name"].to_list() == [" Ann ", "Bob", None, "Ann", "Carol"]


# --- outliers ---------------------------------------------------------------------------------

TEN = pl.DataFrame({"v": [1, 2, 3, 4, 5, 6, 7, 8, 9, 100, None]})
# Nearest percentiles of 1..9, 100: the 10th is 2 and the 90th is 9.
BOUNDS = {"lower_percentile": 10, "upper_percentile": 90}


def test_capping_moves_outliers_to_the_percentile_bounds() -> None:
    frame, result = _run({"type": "outliers", "column": "v", "action": "cap", **BOUNDS}, TEN)
    assert frame["v"].to_list() == [2, 2, 3, 4, 5, 6, 7, 8, 9, 9, None]
    assert frame["v"].dtype == pl.Int64
    assert result.message == "2 outliers outside 2.0 to 9.0"


def test_capping_a_decimal_column_keeps_its_type_and_exact_values() -> None:
    # Bounds whose floats are not exact decimals: 9.95 is 9.949999999999999289... as a float.
    amounts = ["9.95", "12.50", "39.90", None, "4500000.00"]
    frame = pl.DataFrame(
        {"v": [None if a is None else Decimal(a) for a in amounts]},
        schema={"v": pl.Decimal(10, 2)},
    )
    bounds = {"lower_percentile": 0, "upper_percentile": 75}
    capped, result = _run({"type": "outliers", "column": "v", "action": "cap", **bounds}, frame)
    assert result.status == "succeeded", result.error
    assert capped["v"].dtype == pl.Decimal(10, 2)
    assert capped["v"].to_list() == [Decimal(a) for a in ("9.95", "12.50", "39.90")] + [
        None,
        Decimal("39.90"),
    ]
    assert result.message == "1 outliers outside 9.95 to 39.9"


def test_removing_outliers_drops_their_rows_and_keeps_nulls() -> None:
    frame, result = _run({"type": "outliers", "column": "v", "action": "remove", **BOUNDS}, TEN)
    assert frame["v"].to_list() == [2, 3, 4, 5, 6, 7, 8, 9, None]
    assert (result.rows_in, result.rows_out) == (11, 9)


def test_a_nan_is_never_an_outlier_and_never_a_bound() -> None:
    # Nearest percentiles of 1, 2, 3, 100: the 25th is 2 and the 75th is 3.
    frame = pl.DataFrame({"v": [1.0, 2.0, 3.0, float("nan"), 100.0]})
    quartiles = {"lower_percentile": 25, "upper_percentile": 75}
    capped, result = _run({"type": "outliers", "column": "v", "action": "cap", **quartiles}, frame)
    assert capped["v"].to_list()[:3] + capped["v"].to_list()[4:] == [2.0, 2.0, 3.0, 3.0]
    assert result.message == "2 outliers outside 2.0 to 3.0"


def test_flagging_outliers_adds_a_column_and_changes_no_value() -> None:
    frame, _ = _run({"type": "outliers", "column": "v", "action": "flag", **BOUNDS}, TEN)
    assert_series_equal(frame["v"], TEN["v"])
    assert frame["v_outlier"].to_list() == [True] + [False] * 8 + [True, False]


@given(
    st.lists(st.one_of(st.none(), st.floats(-1e6, 1e6)), min_size=1, max_size=60),
    st.floats(0, 49),
    st.floats(51, 100),
)
def test_capping_keeps_every_value_within_the_bounds_and_counts_each_move(
    values: list[float | None], lower: float, upper: float
) -> None:
    frame = pl.DataFrame({"v": values}, schema={"v": pl.Float64})
    definition = {
        "type": "outliers",
        "column": "v",
        "action": "cap",
        "lower_percentile": lower,
        "upper_percentile": upper,
    }
    capped, result = _run(definition, frame)
    low = frame["v"].quantile(lower / 100, interpolation="nearest")
    high = frame["v"].quantile(upper / 100, interpolation="nearest")
    present = capped["v"].drop_nulls()
    if low is not None and high is not None:
        assert present.is_empty() or (present.min() >= low and present.max() <= high)  # type: ignore[operator]
    assert result.values_changed == int(frame["v"].ne_missing(capped["v"]).sum())
    assert capped["v"].null_count() == frame["v"].null_count()


# --- validation -------------------------------------------------------------------------------

ORDERS = pl.DataFrame({"id": [1, 2, 2, 4], "qty": [5, -1, 3, None]})
RULES: list[dict[str, Any]] = [
    {"constraint": "min", "column": "qty", "value": 0},
    {"constraint": "unique", "columns": ["id"]},
]


def test_validation_counts_invalid_rows_and_keeps_them() -> None:
    frame, result = _run({"type": "validate", "constraints": RULES}, ORDERS)
    assert_frame_equal(frame, ORDERS)
    # Row 2 breaks both rules, row 3 only unique: 2 invalid rows.
    assert result.message is not None and result.message.startswith("2 invalid rows")
    assert result.status == "succeeded"


def test_validation_can_drop_the_invalid_rows() -> None:
    frame, result = _run({"type": "validate", "constraints": RULES, "on_invalid": "drop"}, ORDERS)
    assert frame["id"].to_list() == [1, 4]
    assert result.rows_out == 2


def test_validation_uses_the_datasets_constraints_when_it_names_none() -> None:
    context = StepContext(constraints=_constraints(*RULES))
    frame, _ = _run({"type": "validate", "on_invalid": "drop"}, ORDERS, context)
    assert frame["id"].to_list() == [1, 4]


def test_a_critical_constraint_failure_is_reported_not_raised() -> None:
    rules = [{"constraint": "not_null", "column": "qty", "critical": True}]
    frame, result = _run({"type": "validate", "constraints": rules}, ORDERS)
    assert result.status == "failed"
    assert result.error == "critical constraint 1 (not_null) failed: 1 row and 1 value break it"
    assert_frame_equal(frame, ORDERS)


def test_a_critical_failure_does_not_stop_the_step_when_told_not_to() -> None:
    rules = [{"constraint": "not_null", "column": "qty", "critical": True}]
    _, result = _run({"type": "validate", "constraints": rules, "stop_on_critical": False}, ORDERS)
    assert result.status == "succeeded"


# --- values changed, counted by hand ----------------------------------------------------------


@pytest.mark.parametrize(
    ("definition", "frame", "changed"),
    [
        ({"type": "drop_missing"}, PEOPLE, 0),
        # name has one null.
        ({"type": "fill_missing", "columns": ["name"], "method": "mode"}, PEOPLE, 1),
        # age has two nulls, score one.
        ({"type": "fill_missing", "columns": ["age", "score"], "method": "mean"}, PEOPLE, 3),
        ({"type": "fill_missing", "columns": ["age"], "method": "median"}, PEOPLE, 2),
        ({"type": "fill_missing", "columns": ["age"], "method": "value", "value": 1}, PEOPLE, 2),
        # Three present values change type; the null does not count.
        (
            {"type": "convert_type", "column": "n", "to": "integer"},
            pl.DataFrame({"n": ["1", "2", None, "3"]}),
            3,
        ),
        # Already integer: no value differs.
        ({"type": "convert_type", "column": "age", "to": "integer"}, PEOPLE, 0),
        ({"type": "normalize_column_names"}, PEOPLE, 0),
        # " Ann " -> "Ann" only.
        ({"type": "normalize_values", "columns": ["name"], "trim": True}, PEOPLE, 1),
        # " ann ", "BOB" ... lower: " Ann " -> " ann ", "CAROL" -> "carol"; "bob", "ann" stay.
        ({"type": "normalize_values", "columns": ["name"], "case": "lower"}, PEOPLE, 2),
        # Only "bob" is a key.
        ({"type": "normalize_values", "columns": ["name"], "mapping": {"bob": "B"}}, PEOPLE, 1),
        ({"type": "outliers", "column": "v", "action": "cap", **BOUNDS}, TEN, 2),
        ({"type": "outliers", "column": "v", "action": "flag", **BOUNDS}, TEN, 2),
        ({"type": "outliers", "column": "v", "action": "remove", **BOUNDS}, TEN, 0),
        ({"type": "validate", "constraints": RULES, "on_invalid": "drop"}, ORDERS, 0),
    ],
)
def test_values_changed_matches_a_hand_count(
    definition: dict[str, Any], frame: pl.DataFrame, changed: int
) -> None:
    _, result = _run(definition, frame)
    assert result.status == "succeeded"
    assert result.values_changed == changed


def test_the_standard_types_are_registered() -> None:
    assert set(TRANSFORMATIONS) >= {
        "drop_missing",
        "fill_missing",
        "convert_type",
        "normalize_column_names",
        "normalize_values",
        "outliers",
        "validate",
    }


# --- configuration refused at load ------------------------------------------------------------


@pytest.mark.parametrize(
    ("definition", "message"),
    [
        (
            {"type": "fill_missing", "columns": ["name"], "method": "mean"},
            "step 1 (fill_missing): columns: column 'name' holds String, "
            "and a mean needs a number column",
        ),
        (
            {"type": "fill_missing", "columns": ["height"], "method": "median"},
            "step 1 (fill_missing): columns: column 'height' is not in the data",
        ),
        (
            {"type": "outliers", "column": "score", "action": "cap", "upper_percentile": 101},
            "step 1 (outliers): upper_percentile: Input should be less than or equal to 100",
        ),
        (
            {"type": "fill_missing", "columns": ["age"], "method": "value", "value": "old"},
            "step 1 (fill_missing): value: 'old' does not fit column 'age', which holds Int64",
        ),
        (
            {"type": "convert_type", "column": "score", "to": "date"},
            "step 1 (convert_type): to: column 'score' holds Float64 and cannot become date",
        ),
        (
            {"type": "normalize_values", "columns": ["age"], "trim": True},
            "step 1 (normalize_values): columns: column 'age' holds Int64, "
            "and only text is normalised",
        ),
        (
            {"type": "validate", "constraints": [{"constraint": "not_null", "column": "x"}]},
            "step 1 (validate): constraints: constraint 1 (not_null): "
            "column 'x' is not in the data",
        ),
        (
            {
                "type": "outliers",
                "column": "age",
                "action": "cap",
                "lower_percentile": 60,
                "upper_percentile": 40,
            },
            "step 1 (outliers): settings: Value error, lower_percentile must be below "
            "upper_percentile",
        ),
        (
            {"type": "sharpen"},
            "step 1: type: unknown transformation type 'sharpen'",
        ),
        # A value that converting would change or reinterpret is refused, never truncated.
        (
            {"type": "fill_missing", "columns": ["age"], "method": "value", "value": 1.5},
            "step 1 (fill_missing): value: 1.5 does not fit column 'age', which holds Int64",
        ),
        (
            {"type": "fill_missing", "columns": ["age"], "method": "value", "value": True},
            "step 1 (fill_missing): value: True does not fit column 'age', which holds Int64",
        ),
        (
            {"type": "fill_missing", "columns": ["score"], "method": "value", "value": "7"},
            "step 1 (fill_missing): value: '7' does not fit column 'score', which holds Float64",
        ),
        (
            {"type": "fill_missing", "columns": ["name"], "method": "value", "value": 7},
            "step 1 (fill_missing): value: 7 does not fit column 'name', which holds String",
        ),
    ],
)
def test_invalid_configuration_is_refused_naming_step_and_field(
    definition: dict[str, Any], message: str
) -> None:
    with pytest.raises(ConfigError) as refused:
        load_steps([definition], PEOPLE.schema)
    assert str(refused.value).startswith(message)


def test_checks_follow_the_schema_earlier_steps_leave() -> None:
    frame = pl.DataFrame({"Age In Years": [1, None]})
    steps: list[dict[str, Any]] = [
        {"type": "normalize_column_names"},
        {"type": "fill_missing", "columns": ["age_in_years"], "method": "mean"},
    ]
    assert len(load_steps(steps, frame.schema)) == 2
    with pytest.raises(ConfigError, match=r"^step 2 \(fill_missing\): columns: column 'Age"):
        load_steps([steps[0], {**steps[1], "columns": ["Age In Years"]}], frame.schema)
    converted: list[dict[str, Any]] = [
        {"type": "convert_type", "column": "Age In Years", "to": "text"},
        {"type": "fill_missing", "columns": ["Age In Years"], "method": "median"},
    ]
    with pytest.raises(ConfigError, match=r"^step 2 \(fill_missing\): columns: .* holds String"):
        load_steps(converted, frame.schema)


# --- the framework ----------------------------------------------------------------------------


class Double(Transformation):
    """A type the framework has never heard of, defined only here."""

    type = "double_for_test"
    column: str

    def check(self, schema: pl.Schema) -> pl.Schema:
        if self.column not in schema:
            from udp.transformations.base import StepConfigError

            raise StepConfigError("column", f"column '{self.column}' is not in the data")
        return schema

    def apply(self, frame: pl.DataFrame, context: StepContext) -> Applied:
        doubled = frame.with_columns(pl.col(self.column) * 2)
        changed = int(frame[self.column].ne_missing(doubled[self.column]).sum())
        return Applied(doubled, values_changed=changed)


@pytest.fixture
def double() -> Iterator[None]:
    register(Double)
    yield
    del TRANSFORMATIONS[Double.type]


@pytest.mark.usefixtures("double")
def test_a_type_registered_in_a_test_runs_through_the_framework() -> None:
    frame = pl.DataFrame({"v": [1, 0, None, 3]})
    [step] = load_steps([{"type": "double_for_test", "column": "v"}], frame.schema)
    result_frame, result = run_step(frame, step, 1)
    assert result_frame["v"].to_list() == [2, 0, None, 6]
    assert (result.type, result.status, result.values_changed) == (
        "double_for_test",
        "succeeded",
        2,
    )
    with pytest.raises(ConfigError, match=r"^step 1 \(double_for_test\): column: column 'w'"):
        load_steps([{"type": "double_for_test", "column": "w"}], frame.schema)


class Explodes(Transformation):
    type = "explodes_for_test"

    def apply(self, frame: pl.DataFrame, context: StepContext) -> Applied:
        raise RuntimeError("boom")


def test_a_step_that_raises_is_reported_as_failed() -> None:
    frame, result = run_step(PEOPLE, Explodes(), 3)
    assert_frame_equal(frame, PEOPLE)
    assert (result.position, result.status, result.error) == (3, "failed", "RuntimeError: boom")


def test_registering_a_second_class_under_a_taken_name_is_refused() -> None:
    class Impostor(Transformation):
        type = "fill_missing"

    with pytest.raises(ValueError, match="already registered"):
        register(Impostor)


def test_no_step_changes_the_frame_it_was_given() -> None:
    before = PEOPLE.clone()
    for definition in (
        {"type": "fill_missing", "columns": ["age"], "method": "mean"},
        {"type": "normalize_values", "columns": ["name"], "trim": True},
        {"type": "drop_missing"},
    ):
        _run(definition)
    assert_frame_equal(PEOPLE, before)
