"""The profiling engine: every quantity against frames with known answers, then stage tables in
PostgreSQL — stored, compared before and after, sampled, and profiled from the command line."""

import math
import re
import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID

import polars as pl
import psycopg
import pytest
from hypothesis import given
from hypothesis import strategies as st
from psycopg import sql
from typer.testing import CliRunner

from udp.cli import app
from udp.config.source import load_source
from udp.names import Stage, stage_table
from udp.pipeline.load import with_platform_columns
from udp.profiling.frame import (
    ProfileSettings,
    histogram,
    parse_outlier_rule,
    profile_frame,
)
from udp.profiling.models import (
    OutlierRule,
    StageColumnProfile,
    StageProfile,
    compare_profiles,
)
from udp.profiling.stage import describe, profile_stage
from udp.settings import Settings
from udp.storage.loader import ExecutionStart, RunRef, RunStart, StepDefinition
from udp.storage.postgres import PostgresLoader

NOW = datetime(2026, 10, 4, 12, tzinfo=UTC)

# --- a frame with known answers ---------------------------------------------------------------

# Ten rows. Rows 8 and 9 are the same row twice. Every column has one missing value; amount
# holds "abc", which is not an integer, and day holds 30 February, which is not a date.
KNOWN = pl.DataFrame(
    {
        "id": [1, 2, 3, 4, 5, 6, 7, 8, 8, None],
        "name": ["ada", "bo", "cy", "di", "ed", "fi", "gu", "hu", "hu", None],
        "amount": ["10", "11", "12", "13", "14", "15", "abc", "1000", "1000", None],
        "day": [
            "2024-01-01",
            "2024-01-02",
            "2024-02-30",
            "2024-01-04",
            "2024-01-05",
            "2024-01-06",
            "2024-01-07",
            "2024-01-08",
            "2024-01-08",
            None,
        ],
        "ratio": [0.5, 1.0, math.nan, None, 2.0, 2.5, 3.0, 3.5, 3.5, 4.0],
    }
)
DECLARED = ProfileSettings(declared={"amount": "integer", "day": "date"})


def _columns(profile: StageProfile) -> dict[str, StageColumnProfile]:
    return {column.name: column for column in profile.columns}


def test_a_known_frame_is_profiled_exactly() -> None:
    profile = profile_frame(KNOWN, DECLARED)
    columns = _columns(profile)

    # counts
    assert (profile.table_rows, profile.profiled_rows, profile.sampled) == (10, 10, False)
    assert [column.name for column in profile.columns] == ["id", "name", "amount", "day", "ratio"]
    assert [column.type for column in profile.columns] == [
        "bigint",
        "text",
        "text",
        "text",
        "double precision",
    ]
    # missing values
    assert {name: column.missing for name, column in columns.items()} == dict.fromkeys(columns, 1)
    assert profile.missing_values == 5
    # cardinality: the NaN is a value, and so is the text that is not a date
    assert {name: column.distinct for name, column in columns.items()} == dict.fromkeys(columns, 8)
    assert columns["name"].appear_once == 7
    # ranges: the NaN is present but left out of the range
    assert (columns["id"].min, columns["id"].max, columns["id"].mean) == (1, 8, "4.8889")
    assert (columns["ratio"].min, columns["ratio"].max) == (0.5, 4.0)
    assert sum(columns["ratio"].histogram or []) == 8
    # duplicates: no key declared, so on every column
    assert (profile.duplicates, profile.duplicates_by) == (1, "all columns")
    assert profile.duplicate_columns == ["id", "name", "amount", "day", "ratio"]
    # type violations
    assert (columns["amount"].invalid, columns["day"].invalid) == (1, 1)
    assert (profile.invalid_values, profile.missing_required, profile.quality_problems) == (2, 0, 2)
    # outliers: Q1 = 11.75 and Q3 = 261.25 of amount's eight integers, so its fences are
    # 11.75 - 1.5 * 249.5 and 261.25 + 1.5 * 249.5, and both 1000s are above the upper one
    amount = columns["amount"]
    assert (amount.outlier_method, amount.outlier_low, amount.outlier_high) == (
        "iqr",
        -362.5,
        635.5,
    )
    assert {name: column.outliers for name, column in columns.items()} == {
        "id": 0,
        "name": 0,
        "amount": 2,
        "day": 0,
        "ratio": 0,
    }
    assert profile.outliers == 2
    # a date column has a range and no outliers; text has neither
    assert (columns["day"].outlier_method, columns["name"].outlier_method) == (None, None)


def test_duplicates_are_counted_on_the_declared_key_and_rows_without_one_are_not() -> None:
    frame = pl.DataFrame({"id": [1, 1, 2, None, None], "note": ["a", "b", "c", "d", "d"]})

    by_key = profile_frame(frame, ProfileSettings(key=["id"], required=["id"]))
    by_all = profile_frame(frame)
    key_dropped = profile_frame(frame.drop("id"), ProfileSettings(key=["id"]))

    assert (by_key.duplicates, by_key.duplicates_by, by_key.duplicate_columns) == (
        1,
        "key",
        ["id"],
    )
    assert (by_key.missing_required, by_key.quality_problems) == (2, 2)
    assert (by_all.duplicates, by_all.duplicates_by) == (1, "all columns")
    # A key whose column a step dropped cannot be used: the profile says what it used instead.
    assert (key_dropped.duplicates, key_dropped.duplicate_columns) == (1, ["note"])


def test_a_value_that_breaks_its_type_is_a_quality_problem_not_an_outlier() -> None:
    settings = ProfileSettings(declared={"amount": "integer"})

    def amount(values: list[str]) -> StageColumnProfile:
        return _columns(profile_frame(pl.DataFrame({"amount": values}), settings))["amount"]

    both = amount(["10", "11", "12", "13", "14", "abc", "1000"])
    without_extreme = amount(["10", "11", "12", "13", "14", "abc"])
    without_text = amount(["10", "11", "12", "13", "14", "1000"])

    assert (both.invalid, both.outliers) == (1, 1)
    # "abc" breaks the type: a quality problem, and never an outlier.
    assert (without_extreme.invalid, without_extreme.outliers) == (1, 0)
    # 1000 is a perfectly good integer: an outlier, and never a quality problem.
    assert (without_text.invalid, without_text.outliers) == (0, 1)


def test_a_column_that_can_never_be_its_declared_type_is_invalid_throughout() -> None:
    frame = pl.DataFrame({"when": [date(2024, 1, 1), None, date(2024, 1, 3)]})

    (column,) = profile_frame(frame, ProfileSettings(declared={"when": "integer"})).columns

    assert (column.missing, column.invalid, column.outliers) == (1, 2, 0)


# 1..100 with -1000 and 1000 either side: Q1 = 25.25 and Q3 = 75.75 by linear interpolation.
SPREAD = pl.DataFrame({"x": [-1000, *range(1, 101), 1000]})


def _outliers(frame: pl.DataFrame, rule: OutlierRule) -> StageColumnProfile:
    (column,) = profile_frame(frame, ProfileSettings(default_outliers=rule)).columns
    return column


def test_iqr_finds_the_values_beyond_its_fences() -> None:
    tukey = _outliers(SPREAD, OutlierRule())
    tight = _outliers(SPREAD, OutlierRule(method="iqr", k=0.1))

    assert (tukey.outlier_low, tukey.outlier_high, tukey.outliers) == (-50.5, 151.5, 2)
    # Fences 20.2 and 80.8: -1000 and 1..20 below, 81..100 and 1000 above.
    assert tight.outlier_low == pytest.approx(20.2)
    assert tight.outlier_high == pytest.approx(80.8)
    assert tight.outliers == 42


def test_percentile_finds_the_values_beyond_its_percentiles() -> None:
    hundred = pl.DataFrame({"x": list(range(1, 101))})

    default = _outliers(hundred, OutlierRule(method="percentile"))
    wide = _outliers(hundred, OutlierRule(method="percentile", lower=5, upper=95))
    nothing = _outliers(hundred, OutlierRule(method="none"))

    # The 1st and 99th percentiles are 1.99 and 99.01: only 1 and 100 are beyond them.
    assert default.outlier_low == pytest.approx(1.99)
    assert default.outlier_high == pytest.approx(99.01)
    assert default.outliers == 2
    # 5.95 and 95.05: 1..5 and 96..100.
    assert wide.outliers == 10
    assert (nothing.outlier_method, nothing.outlier_low, nothing.outliers) == ("none", None, 0)


def test_each_column_can_have_its_own_outlier_rule() -> None:
    frame = SPREAD.with_columns(pl.col("x").alias("y"))
    settings = ProfileSettings(outliers={"y": OutlierRule(method="percentile", lower=5, upper=95)})

    x, y = profile_frame(frame, settings).columns

    assert (x.outlier_method, x.outliers) == ("iqr", 2)
    assert y.outlier_method == "percentile"


NUMBERS = st.lists(
    st.one_of(
        st.none(),
        st.floats(allow_nan=True, allow_infinity=True),
        st.integers(-1000, 1000).map(float),
    ),
    max_size=40,
)
RULES = st.one_of(
    st.floats(min_value=0.01, max_value=10).map(lambda k: OutlierRule(method="iqr", k=k)),
    st.tuples(st.floats(0, 100), st.floats(0, 100))
    .filter(lambda pair: pair[0] < pair[1])
    .map(lambda pair: OutlierRule(method="percentile", lower=pair[0], upper=pair[1])),
)


@given(NUMBERS, RULES)
def test_outliers_are_exactly_the_finite_values_beyond_the_reported_bounds(
    values: list[float | None], rule: OutlierRule
) -> None:
    column = _outliers(pl.DataFrame({"x": values}, schema={"x": pl.Float64}), rule)

    finite = [value for value in values if value is not None and math.isfinite(value)]
    assert column.missing == sum(value is None for value in values)
    if not finite:
        assert (column.outlier_low, column.outliers) == (None, 0)
        return
    assert column.outlier_low is not None and column.outlier_high is not None
    beyond = [v for v in finite if v < column.outlier_low or v > column.outlier_high]
    assert column.outliers == len(beyond)


@given(NUMBERS)
def test_the_whole_range_never_has_an_outlier(values: list[float | None]) -> None:
    column = _outliers(
        pl.DataFrame({"x": values}, schema={"x": pl.Float64}),
        OutlierRule(method="percentile", lower=0, upper=100),
    )

    assert column.outliers == 0


@given(
    st.lists(
        st.one_of(
            st.none(),
            st.integers(-(10**20), 10**20).map(str),
            st.text(max_size=6),
            st.sampled_from(["", " 1", "+7", "1e3", "0x10", "١٢"]),
        ),
        max_size=40,
    )
)
def test_every_value_is_missing_invalid_or_valid_and_only_valid_ones_are_outliers(
    values: list[str | None],
) -> None:
    frame = pl.DataFrame({"n": values}, schema={"n": pl.String})

    (column,) = profile_frame(frame, ProfileSettings(declared={"n": "integer"})).columns

    valid = len(values) - column.missing - column.invalid
    assert column.missing == sum(value is None for value in values)
    assert 0 <= column.outliers <= valid
    # An integer is optional sign and ASCII digits, and fits the 64 bits an integer column holds.
    assert valid == sum(
        value is not None
        and re.fullmatch(r"[+-]?[0-9]+", value) is not None
        and -(2**63) <= int(value) < 2**63
        for value in values
    )


def test_histogram_bars_are_drawn_as_the_dashboard_draws_them() -> None:
    values = pl.Series([-3.5, 0, 0, 7.25, 100])

    bars = histogram(values)

    # Bars are 5.175 wide from -3.5, so -3.5 and both zeros share the first; 100 is the last.
    assert (len(bars), sum(bars), bars[0], bars[-1]) == (20, 5, 3, 1)
    assert histogram(pl.Series([2.5] * 7)) == [7] + [0] * 19
    assert histogram(pl.Series([], dtype=pl.Float64)) == [0] * 20


def test_an_empty_table_has_zero_of_everything() -> None:
    frame = pl.DataFrame(schema={"v": pl.Int64, "t": pl.String})

    profile = profile_frame(frame, ProfileSettings(key=["v"]))

    assert (profile.table_rows, profile.duplicates, profile.outliers) == (0, 0, 0)
    number, words = profile.columns
    assert (number.missing, number.min, number.histogram, number.outlier_low) == (
        0,
        None,
        [0] * 20,
        None,
    )
    assert (words.distinct, words.all_values, words.pattern) == (0, [], None)


def test_text_columns_list_their_values_and_pattern_as_the_dashboard_does() -> None:
    frame = pl.DataFrame(
        {
            "code": [f"AB-{100 + i}" for i in range(1, 20)] + ["odd one"],
            "flag": [i % 2 == 0 for i in range(20)],
        }
    )

    code, flag = profile_frame(frame).columns

    assert (code.pattern, code.pattern_share) == ("^[A-Z]{2}\\-[0-9]{3}$", 0.95)
    assert [v.value for v in code.most_used or []] == ["AB-101", "AB-102", "AB-103"]
    assert code.least_used is None
    assert [(v.value, v.count) for v in flag.all_values or []] == [("false", 10), ("true", 10)]
    assert flag.pattern is None


def test_the_settings_come_from_the_dataset_declaration(tmp_path: Path) -> None:
    folder = tmp_path / "shop"
    folder.mkdir()
    (folder / "source.yaml").write_text(
        "connection:\n  type: csv\ndatasets:\n  - name: orders\n    path: orders.csv\n"
        "    load_mode: merge\n    watermark: id\n    primary_key: [id]\n"
        "    columns:\n      id: integer\n      amount: decimal(10,2)\n"
        "    checks:\n      - check: not_null\n        column: email\n"
        "      - check: not_null\n        column: id\n",
        encoding="utf-8",
    )
    (dataset,) = load_source(tmp_path, "shop", {}).datasets

    settings = ProfileSettings.for_dataset(dataset, {"amount": OutlierRule(method="none")})

    assert settings.declared == {"id": "integer", "amount": "decimal(10,2)"}
    assert (settings.key, settings.required) == (("id",), ("id", "email"))
    assert settings.outliers == {"amount": OutlierRule(method="none")}
    assert settings.default_outliers == OutlierRule(method="iqr", k=1.5)


@pytest.mark.parametrize(
    ("text", "column", "rule"),
    [
        ("iqr", None, OutlierRule()),
        ("amount=iqr:3", "amount", OutlierRule(k=3)),
        ("percentile", None, OutlierRule(method="percentile")),
        (
            " price = percentile:5:95",
            "price",
            OutlierRule(method="percentile", lower=5, upper=95),
        ),
        ("id=none", "id", OutlierRule(method="none")),
    ],
)
def test_an_outlier_rule_is_read_from_its_text(
    text: str, column: str | None, rule: OutlierRule
) -> None:
    assert parse_outlier_rule(text) == (column, rule)


@pytest.mark.parametrize(
    ("text", "problem"),
    [
        ("zscore", "is not iqr"),
        ("iqr:0", "greater than 0"),
        ("iqr:1:2", "is not iqr"),
        ("percentile:95:5", "must be below"),
        ("percentile:5", "is not iqr"),
        ("percentile:-1:50", "greater than or equal to 0"),
        ("iqr:many", "are numbers"),
        ("=iqr", "names no column"),
        ("none:1", "is not iqr"),
    ],
)
def test_a_bad_outlier_rule_is_refused_saying_why(text: str, problem: str) -> None:
    with pytest.raises(ValueError, match=problem):
        parse_outlier_rule(text)


def test_two_profiles_compare_before_and_after() -> None:
    before = profile_frame(KNOWN, DECLARED)
    fixed = KNOWN.filter(pl.col("id").is_not_null()).unique(maintain_order=True)
    fixed = fixed.filter(~pl.col("amount").is_in(["abc"]) & (pl.col("day") != "2024-02-30"))
    after = profile_frame(fixed, DECLARED)

    comparison = compare_profiles(before, after).model_dump()

    assert comparison == {
        "rows": {"before": 10, "after": 6},
        "missing_values": {"before": 5, "after": 1},
        "invalid_values": {"before": 2, "after": 0},
        "outliers": {"before": 2, "after": 1},
        "duplicates": {"before": 1, "after": 0},
    }


def test_a_profile_reads_as_text_a_person_can_follow() -> None:
    text = describe(profile_frame(KNOWN, DECLARED), "shop.orders at raw")

    lines = text.splitlines()
    assert lines[:5] == [
        "shop.orders at raw: 10 rows, every one profiled",
        "Duplicates: 1, by all columns (id, name, amount, day, ratio)",
        "Missing values: 5",
        "Data-quality problems: 2 (2 values that do not fit their type, 0 required values missing)",
        "Statistical outliers: 2",
    ]
    header = ["column", "type", "missing", "distinct", "invalid", "outliers", "range"]
    assert lines[6].split() == header
    assert lines[9].split() == ["amount", "text", "as", "integer", "1", "8", "1", "2", "(iqr)"]


# --- stage tables in PostgreSQL ----------------------------------------------------------------


@pytest.fixture
def loader() -> Iterator[PostgresLoader]:
    with PostgresLoader(Settings().database_url) as made:  # type: ignore[call-arg]
        yield made


@pytest.fixture
def source(loader: PostgresLoader) -> Iterator[str]:
    """A throwaway source name; its stage tables are dropped afterwards."""
    name = f"prf{uuid.uuid4().hex[:10]}"
    yield name
    with psycopg.connect(Settings().database_url, autocommit=True) as conn:  # type: ignore[call-arg]
        for stage in Stage:
            for dataset in ("orders", "big"):
                conn.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}").format(
                        sql.Identifier(*stage_table(stage, name, dataset))
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


@pytest.mark.db
def test_two_stages_are_profiled_stored_and_compared(loader: PostgresLoader, source: str) -> None:
    run_id = _ingest(loader, source, KNOWN)
    execution_id = uuid.uuid7()
    with loader.stages() as stages:
        pipeline = stages.save_pipeline(
            source, "orders", "tidy", {}, [StepDefinition("drop_bad_rows")], NOW
        )
        stages.start_execution(
            ExecutionStart(execution_id, pipeline.pipeline_id, pipeline.version, "manual", NOW)
        )
        copied = stages.start_staging(source, "orders")
    staging = sql.Identifier(*stage_table(Stage.STAGING, source, "orders"))
    with psycopg.connect(Settings().database_url, autocommit=True) as conn:  # type: ignore[call-arg]
        conn.execute(
            sql.SQL(
                "DELETE FROM {t} WHERE id IS NULL OR amount = 'abc' OR day = '2024-02-30' "
                "OR ctid NOT IN (SELECT min(ctid) FROM {t} GROUP BY id)"
            ).format(t=staging)
        )
    with loader.stages() as stages:
        stages.finish_execution(execution_id, ended_at=NOW, rows_in=copied, rows_out=6)

    with loader.stages() as stages:
        assert stages.newest_run(Stage.RAW, source, "orders") == RunRef(ingest_run_id=run_id)
        assert stages.newest_run(Stage.CLEAN, source, "orders") == RunRef(execution_id=execution_id)
        raw = stages.profile(
            Stage.RAW,
            source,
            "orders",
            RunRef(ingest_run_id=run_id),
            profiled_at=NOW,
            settings=DECLARED,
        )
        clean = stages.profile(
            Stage.CLEAN,
            source,
            "orders",
            RunRef(execution_id=execution_id),
            profiled_at=NOW,
            settings=DECLARED,
        )
    with loader.stages() as stages:
        comparison = stages.compare_profiles(raw.profile_id, clean.profile_id)
        stored = stages.read_profiles(source, "orders")

    assert comparison.model_dump() == {
        "rows": {"before": 10, "after": 6},
        "missing_values": {"before": 5, "after": 1},
        "invalid_values": {"before": 2, "after": 0},
        "outliers": {"before": 2, "after": 1},
        "duplicates": {"before": 1, "after": 0},
    }
    assert [(item.profile.stage, item.profile.run) for item in stored] == [
        (Stage.RAW, RunRef(ingest_run_id=run_id)),
        (Stage.CLEAN, RunRef(execution_id=execution_id)),
    ]
    # What was stored is what the engine gives for the same rows, without the database.
    assert StageProfile.model_validate(stored[0].profile.result) == profile_frame(
        KNOWN, DECLARED, types=[(name, kind) for name, kind in _stored_types(source)]
    )
    with loader.stages() as stages, pytest.raises(LookupError, match="no profile"):
        stages.compare_profiles(raw.profile_id, -1)


def _stored_types(source: str) -> list[tuple[str, str]]:
    with psycopg.connect(Settings().database_url) as conn:  # type: ignore[call-arg]
        rows = conn.execute(
            "SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute "
            "WHERE attrelid = to_regclass(%s) AND attnum > 0 AND NOT attisdropped "
            "ORDER BY attnum",
            [".".join(stage_table(Stage.RAW, source, "orders"))],
        ).fetchall()
    return [(name, kind) for name, kind in rows]


@pytest.mark.db
def test_every_stored_type_is_read_and_profiled(loader: PostgresLoader, source: str) -> None:
    schema, table = stage_table(Stage.RAW, source, "orders")
    with psycopg.connect(Settings().database_url, autocommit=True) as conn:  # type: ignore[call-arg]
        target = sql.Identifier(schema, table)
        conn.execute(
            sql.SQL(
                "CREATE TABLE {} (s smallint, d numeric(8,2), wide numeric, f float8, day date, "
                "at timestamptz, local timestamp, flag boolean, word varchar(5), doc jsonb, "
                "id uuid)"
            ).format(target)
        )
        conn.execute(
            sql.SQL(
                "INSERT INTO {} VALUES (1, 1.25, '1e400', 'NaN', 'infinity', "
                "'2024-01-01 10:00+02', '2024-01-01 10:00', true, 'a', %s, gen_random_uuid()), "
                "(2, 2.50, 7, 1.5, '2024-01-02', NULL, NULL, false, 'b', NULL, NULL)"
            ).format(target),
            ['{"k": 1}'],
        )
        profile = profile_stage(conn, Stage.RAW, source, "orders")

    columns = _columns(profile)
    assert (columns["s"].type, columns["s"].min, columns["s"].max) == ("smallint", 1, 2)
    assert (columns["d"].min, columns["d"].max) == ("1.25", "2.50")
    assert (columns["wide"].kind, columns["wide"].max) == ("number", 7.0)
    assert (columns["f"].missing, columns["f"].min) == (0, 1.5)
    # An infinite date cannot be held in Python: it is read as no value.
    assert (columns["day"].missing, columns["day"].min) == (1, "2024-01-02")
    assert columns["at"].min == "2024-01-01T08:00:00+00:00"
    assert columns["local"].min == "2024-01-01T10:00:00"
    assert [v.value for v in columns["flag"].all_values or []] == ["false", "true"]
    assert [v.value for v in columns["word"].all_values or []] == ["a", "b"]
    assert (columns["doc"].kind, columns["doc"].missing) == ("other", 1)
    assert (columns["id"].kind, columns["id"].distinct) == ("other", 1)


@pytest.mark.db
def test_a_table_over_a_million_rows_is_profiled_on_a_sample(
    loader: PostgresLoader, source: str
) -> None:
    schema, table = stage_table(Stage.RAW, source, "big")
    with psycopg.connect(Settings().database_url, autocommit=True) as conn:  # type: ignore[call-arg]
        conn.execute(
            sql.SQL("CREATE TABLE {} AS SELECT i FROM generate_series(1, 1000500) AS i").format(
                sql.Identifier(schema, table)
            )
        )
        profile = profile_stage(conn, Stage.RAW, source, "big")
        left = conn.execute("SELECT to_regclass('pg_temp.profile_sample')").fetchone()

    assert (profile.table_rows, profile.sampled) == (1_000_500, True)
    # At most the limit, and a Bernoulli draw of 110% of it falls short only by very bad luck.
    assert 990_000 <= profile.profiled_rows <= 1_000_000
    (i,) = profile.columns
    assert i.distinct == profile.profiled_rows and i.missing == 0
    assert describe(profile, "x").splitlines()[0] == (
        f"x: 1,000,500 rows, sampled: {profile.profiled_rows:,} profiled"
    )
    assert left == (None,)


def _write_source(sources_dir: Path, name: str) -> None:
    folder = sources_dir / name
    folder.mkdir(parents=True)
    (folder / "source.yaml").write_text(
        "connection:\n  type: csv\ndatasets:\n  - name: orders\n    path: orders.csv\n"
        "    load_mode: merge\n    watermark: id\n    primary_key: [id]\n"
        "    columns:\n      amount: integer\n      day: date\n",
        encoding="utf-8",
    )


@pytest.mark.db
def test_the_profile_command_prints_and_stores_a_profile(
    loader: PostgresLoader, source: str, tmp_path: Path
) -> None:
    sources_dir = tmp_path / "sources"
    _write_source(sources_dir, source)
    run_id = _ingest(loader, source, KNOWN)

    result = CliRunner().invoke(
        app,
        ["profile", source, "orders", "--stage", "raw", "--outliers", "ratio=percentile:5:95"],
        env={"UDP_SOURCES_DIR": str(sources_dir)},
    )

    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    assert f"{source}.orders at raw: 10 rows, every one profiled" in lines
    # The key the source declares, and the required value missing from it.
    assert "Duplicates: 1, by key (id)" in lines
    assert (
        "Data-quality problems: 3 (2 values that do not fit their type, 1 required values missing)"
        in lines
    )
    with loader.stages() as stages:
        (stored,) = stages.read_profiles(source, "orders")
    assert lines[-1] == f"Stored as profile {stored.profile_id}, of ingest run {run_id}."
    assert (stored.profile.stage, stored.profile.run) == (Stage.RAW, RunRef(ingest_run_id=run_id))
    ratio = _columns(StageProfile.model_validate(stored.profile.result))["ratio"]
    assert ratio.outlier_method == "percentile"


@pytest.mark.db
def test_the_profile_command_refuses_a_stage_no_run_has_made(
    loader: PostgresLoader, source: str, tmp_path: Path
) -> None:
    sources_dir = tmp_path / "sources"
    _write_source(sources_dir, source)
    _ingest(loader, source, KNOWN)
    env = {"UDP_SOURCES_DIR": str(sources_dir)}

    clean = CliRunner().invoke(app, ["profile", source, "orders", "--stage", "clean"], env=env)
    unknown = CliRunner().invoke(app, ["profile", source, "lost"], env=env)

    assert clean.exit_code == 1
    assert f"no run has made the clean table of {source}.orders yet" in clean.output
    assert unknown.exit_code == 2
    assert f"source '{source}' has no dataset 'lost'" in unknown.output


@pytest.mark.db
def test_the_profile_command_profiles_the_raw_table_the_run_command_made(
    source: str, tmp_path: Path
) -> None:
    sources_dir = tmp_path / "sources"
    _write_source(sources_dir, source)
    orders = sources_dir / source / "orders.csv"
    env = {"UDP_SOURCES_DIR": str(sources_dir)}
    runner = CliRunner()
    raw = sql.Identifier(*stage_table(Stage.RAW, source, "orders"))

    def run(rows: str, *options: str) -> list[tuple[str, int]]:
        """Load the rows; returns RAW's row count per ingest run, oldest first."""
        orders.write_text(f"id,name,amount,day\n{rows}", encoding="utf-8")
        loaded = runner.invoke(app, ["run", source, *options], env=env)
        assert loaded.exit_code == 0, loaded.output
        with psycopg.connect(Settings().database_url) as conn:  # type: ignore[call-arg]
            counted = conn.execute(
                sql.SQL(
                    "SELECT _run_id::text, count(*) FROM {} GROUP BY _run_id "
                    "ORDER BY min(_loaded_at)"
                ).format(raw)
            ).fetchall()
        return [(run_id, count) for run_id, count in counted]

    (first,) = run("1,ada,10,2024-01-01\n2,bo,11,2024-01-02\n")
    profiled = runner.invoke(app, ["profile", source, "orders"], env=env)

    assert profiled.exit_code == 0, profiled.output
    lines = profiled.output.splitlines()
    assert f"{source}.orders at raw: 2 rows, every one profiled" in lines
    assert lines[-1].endswith(f"of ingest run {first[0]}.")
    assert first[1] == 2

    # A merge reads from the saved watermark, id 2, again: RAW gains ids 2 and 3 and keeps the
    # first run's rows as they were.
    kept, added = run("1,ada,10,2024-01-01\n2,bo,12,2024-01-02\n3,cy,13,2024-01-03\n")
    assert kept == first
    assert added[1] == 2

    (refreshed,) = run("4,di,14,2024-01-04\n", "--full-refresh")
    assert refreshed[1] == 1


def test_the_profile_command_refuses_a_bad_outlier_rule_before_connecting(
    tmp_path: Path,
) -> None:
    sources_dir = tmp_path / "sources"
    _write_source(sources_dir, "shop")

    result = CliRunner().invoke(
        app,
        ["profile", "shop", "orders", "--outliers", "amount=zscore"],
        env={
            "UDP_SOURCES_DIR": str(sources_dir),
            "UDP_DATABASE_URL": "postgresql://x@127.0.0.1:1/x",
        },
    )

    assert result.exit_code == 2
    assert "is not iqr" in result.output
