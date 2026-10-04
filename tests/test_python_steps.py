"""Custom Python scripts as pipeline steps: the contract, failures, output and reproducibility."""

from hashlib import sha256
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from udp.errors import ConfigError
from udp.transformations import StepResult, load_steps, run_step, run_steps
from udp.transformations.python_step import OUTPUT_LIMIT, PythonStep

EXAMPLE = Path(__file__).parents[1] / "scripts" / "custom" / "customer_transform.py"

PEOPLE = pl.DataFrame(
    {
        "name": ["ann", "bob", "carol"],
        "age": [30, None, 25],
    }
)
# The start of a script whose transform uses polars; each test adds the body.
WITH_POLARS = "import polars as pl\n\ndef transform(df):\n"


@pytest.fixture(autouse=True)
def in_a_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Scripts are named relative to the project folder, as a pipeline names them."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "scripts").mkdir()


def _script(code: str, name: str = "step.py") -> str:
    path = Path("scripts") / name
    path.write_bytes(code.encode())
    return path.as_posix()


def _run(script: str, frame: pl.DataFrame = PEOPLE, **settings: Any) -> StepResult:
    (step,) = load_steps([{"type": "python", "script": script, **settings}], frame.schema)
    return run_step(frame, step, 1)[1]


# --- a working script -----------------------------------------------------------------------


def test_a_working_script_changes_the_frame_and_records_the_code_it_ran() -> None:
    code = WITH_POLARS + "    return df.with_columns(adult=pl.col('age') >= 18)\n"
    script = _script(code)
    (step,) = load_steps([{"type": "python", "script": script}], PEOPLE.schema)

    frame, result = run_step(PEOPLE, step, 1)

    assert frame["adult"].to_list() == [True, None, True]
    assert result.status == "succeeded"
    assert (result.rows_in, result.rows_out) == (3, 3)
    assert result.script_sha256 == sha256(code.encode()).hexdigest()
    assert result.message == "columns added: adult"
    assert result.duration_seconds > 0
    assert result.error is None and result.error_line is None


def test_values_changed_counts_the_cells_a_script_changed() -> None:
    script = _script(WITH_POLARS + "    return df.with_columns(age=pl.col('age').fill_null(0))\n")
    assert _run(script).values_changed == 1


def test_a_script_may_drop_rows() -> None:
    script = _script("def transform(df):\n    return df.drop_nulls()\n")
    result = _run(script)
    assert (result.status, result.rows_in, result.rows_out) == ("succeeded", 3, 2)


def test_the_timeout_is_900_seconds_unless_the_step_sets_one() -> None:
    script = _script("def transform(df):\n    return df\n")
    assert PythonStep(script=script).timeout == 900
    (step,) = load_steps([{"type": "python", "script": script, "timeout": 5}], PEOPLE.schema)
    assert isinstance(step, PythonStep) and step.timeout == 5


# --- a script that fails ----------------------------------------------------------------------


def test_a_script_that_raises_fails_with_its_message_and_line() -> None:
    script = _script("def transform(df):\n    total = 1\n    raise ValueError('none today')\n")
    result = _run(script)
    assert result.status == "failed"
    assert result.error == f"{script}: transform failed: ValueError: none today (line 3)"
    assert result.error_line == 3
    assert result.script_sha256 is not None
    assert result.rows_out == result.rows_in == 3


def test_the_line_is_the_scripts_own_even_when_the_error_is_raised_deeper() -> None:
    script = _script(WITH_POLARS + "    return df.select(pl.col('nope'))\n")
    result = _run(script)
    assert result.status == "failed"
    assert "ColumnNotFoundError" in (result.error or "")
    assert result.error_line == 4


def test_a_script_that_does_not_compile_fails_naming_the_line() -> None:
    script = _script("def transform(df):\n    return df +\n")
    result = _run(script)
    assert result.status == "failed"
    assert result.error is not None and "could not be loaded: SyntaxError" in result.error
    assert result.error_line == 2


def test_a_script_without_transform_fails_saying_so() -> None:
    script = _script("def tranform(df):\n    return df\n")
    assert _run(script).error == f"{script}: defines no function 'transform'"


def test_a_script_that_calls_exit_fails_its_step() -> None:
    script = _script("import sys\n\ndef transform(df):\n    sys.exit(0)\n")
    result = _run(script)
    assert result.status == "failed"
    assert result.error == f"{script}: transform failed: SystemExit: 0 (line 4)"


def test_a_script_whose_process_dies_fails_with_the_exit_code() -> None:
    script = _script("import os\n\ndef transform(df):\n    os._exit(7)\n")
    result = _run(script)
    assert result.error == f"{script}: the script's process ended with exit code 7"


@pytest.mark.parametrize(
    ("returned", "named"),
    [("{'a': [1]}", "dict"), ("df.lazy()", "LazyFrame"), ("None", "NoneType")],
)
def test_a_script_returning_something_else_than_a_frame_fails(returned: str, named: str) -> None:
    script = _script(f"def transform(df):\n    return {returned}\n")
    result = _run(script)
    assert result.status == "failed"
    assert result.error == f"{script}: transform returned {named}, expected a polars DataFrame"


@pytest.mark.parametrize(
    ("result", "reason"),
    [
        ("df.rename({'age': 'Age In Years'})", "not a clean column name"),
        ("df.with_columns(_loaded_at=pl.lit(1))", "is a platform column"),
        ("df.select([])", "returned no columns"),
    ],
)
def test_a_script_returning_unstorable_columns_fails(result: str, reason: str) -> None:
    script = _script(f"import polars as pl\n\ndef transform(df):\n    return {result}\n")
    outcome = _run(script)
    assert outcome.status == "failed"
    assert outcome.error is not None and reason in outcome.error


def test_a_script_that_never_returns_is_stopped_at_its_timeout() -> None:
    script = _script(
        "import time\n\ndef transform(df):\n    print('working on it')\n    time.sleep(60)\n"
    )
    result = _run(script, timeout=4)
    assert result.status == "failed"
    assert result.error == f"{script}: took longer than 4s and was stopped"
    assert result.duration_seconds < 30
    assert "working on it" in (result.output or "")


# --- the script's file ------------------------------------------------------------------------


def test_a_missing_script_is_refused_when_the_steps_are_loaded() -> None:
    with pytest.raises(ConfigError, match=r"step 1 \(python\): script: scripts/gone.py is not a"):
        load_steps([{"type": "python", "script": "scripts/gone.py"}], PEOPLE.schema)


def test_a_script_deleted_after_loading_fails_its_step() -> None:
    script = _script("def transform(df):\n    return df\n")
    (step,) = load_steps([{"type": "python", "script": script}], PEOPLE.schema)
    Path(script).unlink()
    frame, result = run_step(PEOPLE, step, 1)
    assert result.status == "failed"
    assert result.error == f"{script}: the script is not there"
    assert result.script_sha256 is None
    assert_frame_equal(frame, PEOPLE)


def test_a_script_edited_between_runs_records_a_different_hash() -> None:
    script = _script("def transform(df):\n    return df\n")
    (step,) = load_steps([{"type": "python", "script": script}], PEOPLE.schema)
    first = run_step(PEOPLE, step, 1)[1]
    Path(script).write_text("def transform(df):\n    return df.head(1)\n")
    second = run_step(PEOPLE, step, 1)[1]
    assert first.script_sha256 != second.script_sha256
    assert second.script_sha256 == sha256(Path(script).read_bytes()).hexdigest()
    assert (first.rows_out, second.rows_out) == (3, 1)


# --- in a pipeline ----------------------------------------------------------------------------


def _steps(*definitions: dict[str, Any]) -> list[Any]:
    return load_steps(list(definitions), PEOPLE.schema)


def test_a_failing_script_fails_only_its_step_and_keeps_earlier_results() -> None:
    failing = _script("def transform(df):\n    raise RuntimeError('bad data')\n", "fail.py")
    steps = _steps(
        {"type": "fill_missing", "columns": ["age"], "method": "value", "value": 0},
        {"type": "python", "script": failing},
        {"type": "normalize_values", "columns": ["name"], "case": "upper"},
    )

    frame, results = run_steps(PEOPLE, steps)

    assert [(r.position, r.type, r.status) for r in results] == [
        (1, "fill_missing", "succeeded"),
        (2, "python", "failed"),
    ]
    assert results[0].values_changed == 1
    assert results[1].error == f"{failing}: transform failed: RuntimeError: bad data (line 2)"
    assert results[1].error_line == 2
    assert frame["age"].to_list() == [30, 0, 25]
    assert frame["name"].to_list() == ["ann", "bob", "carol"]


def test_continue_hands_the_frame_from_before_the_failed_step_to_the_next() -> None:
    failing = _script("def transform(df):\n    raise RuntimeError('bad data')\n", "fail.py")
    steps = _steps(
        {"type": "fill_missing", "columns": ["age"], "method": "value", "value": 0},
        {"type": "python", "script": failing},
        {"type": "normalize_values", "columns": ["name"], "case": "upper"},
    )

    frame, results = run_steps(PEOPLE, steps, on_failure="continue")

    assert [r.status for r in results] == ["succeeded", "failed", "succeeded"]
    assert frame["age"].to_list() == [30, 0, 25]
    assert frame["name"].to_list() == ["ANN", "BOB", "CAROL"]


def test_a_script_that_renames_a_column_fails_the_later_step_that_needs_it() -> None:
    renaming = _script("def transform(df):\n    return df.rename({'age': 'years'})\n", "rename.py")
    steps = _steps(
        {"type": "python", "script": renaming},
        {"type": "fill_missing", "columns": ["age"], "method": "value", "value": 0},
    )

    frame, results = run_steps(PEOPLE, steps)

    assert results[0].status == "succeeded"
    assert results[0].message == "columns added: years; columns removed: age"
    assert results[1].status == "failed"
    assert results[1].error == "columns: column 'age' is not in the data"
    assert frame.columns == ["name", "years"]


def test_the_same_pipeline_twice_gives_the_same_output_and_hashes() -> None:
    script = _script(
        "import polars as pl\n\n"
        "def transform(df):\n"
        "    ranked = df.with_columns(rank=pl.col('age').rank('ordinal').cast(pl.Int64))\n"
        "    return ranked.sort('name', descending=True)\n"
    )
    steps = _steps(
        {"type": "fill_missing", "columns": ["age"], "method": "mean"},
        {"type": "python", "script": script},
    )

    first, first_results = run_steps(PEOPLE, steps)
    second, second_results = run_steps(PEOPLE, steps)

    assert [r.status for r in first_results] == ["succeeded", "succeeded"], first_results
    assert_frame_equal(first, second)
    assert first["name"].to_list() == ["carol", "bob", "ann"]
    hashes = [r.script_sha256 for r in first_results]
    assert hashes == [r.script_sha256 for r in second_results]
    assert hashes[1] == sha256(Path(script).read_bytes()).hexdigest()


# --- output -----------------------------------------------------------------------------------


def test_what_a_script_prints_is_kept_with_its_step() -> None:
    script = _script(
        "import logging, sys\n\n"
        "def transform(df):\n"
        "    print(f'{df.height} rows came in')\n"
        "    print('a warning', file=sys.stderr)\n"
        "    logging.getLogger('mine').warning('logged too')\n"
        "    return df\n"
    )
    result = _run(script)
    assert result.status == "succeeded"
    assert result.output is not None
    assert "3 rows came in" in result.output
    assert "a warning" in result.output
    assert "logged too" in result.output


def test_what_a_failing_script_printed_is_kept_too() -> None:
    script = _script("def transform(df):\n    print('about to fail')\n    raise KeyError('x')\n")
    result = _run(script)
    assert result.status == "failed"
    assert "about to fail" in (result.output or "")


@pytest.mark.parametrize("printed", [250_000, 2_000_000])
def test_a_script_that_prints_a_lot_keeps_the_end_of_its_output(printed: int) -> None:
    """2,000,000 characters is more than the part of the file's end that is read at all."""
    script = _script(
        f"def transform(df):\n    print('x' * {printed})\n    print('the end')\n    return df\n"
    )
    output = _run(script).output or ""
    assert output.startswith("[earlier output left out; the script printed ")
    assert output.rstrip().endswith("x\nthe end")
    assert len(output) < OUTPUT_LIMIT + 100


def test_a_nul_character_in_the_output_is_dropped() -> None:
    script = _script("def transform(df):\n    print('a\\x00b')\n    return df\n")
    assert _run(script).output == "ab\n"


# --- the documented example -------------------------------------------------------------------


def test_the_documented_example_runs() -> None:
    customers = pl.DataFrame({"id": [1, 2, 3], "email": [" Ann@Shop.NL ", None, "bob@x.com"]})
    (step,) = load_steps([{"type": "python", "script": str(EXAMPLE)}], customers.schema)

    frame, result = run_step(customers, step, 1)

    assert result.status == "succeeded", result.error
    assert frame["email"].to_list() == ["ann@shop.nl", None, "bob@x.com"]
    assert frame["email_domain"].to_list() == ["shop.nl", None, "x.com"]
    assert result.output == "1 customers have no email\n"
