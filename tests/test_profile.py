import re
from collections.abc import Iterator
from datetime import date, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

import psycopg
import pytest
from hypothesis import given
from hypothesis import strategies as st
from psycopg import sql
from psycopg.rows import dict_row

from udp.api.catalog import json_value
from udp.api.models import ColumnProfile
from udp.api.profile import (
    HISTOGRAM_BARS,
    SHOWN_LENGTH,
    best_pattern,
    kind_of,
    profile_table,
    shape_of,
    shape_regex,
)
from udp.settings import Settings

# --- shapes and patterns, no database -------------------------------------------------------

TEXT = st.text(
    alphabet=st.one_of(
        st.sampled_from("aZ09-_. /+*?()[]{}^$|\\\n"),
        st.characters(codec="utf-8"),
    ),
    max_size=20,
)


@given(TEXT)
def test_the_pattern_of_a_value_matches_that_value(value: str) -> None:
    assert re.fullmatch(shape_regex(shape_of(value)), value, re.DOTALL)


@given(TEXT, TEXT)
def test_the_exact_pattern_refuses_a_value_of_another_shape(value: str, other: str) -> None:
    matched = re.fullmatch(shape_regex(shape_of(value)), other, re.DOTALL) is not None

    assert matched == (shape_of(other) == shape_of(value))


# Values from a small alphabet, so that shapes repeat and line up often enough for the rule
# about lining up to be exercised, alongside values of any shape.
COLUMN = st.lists(
    st.one_of(st.text(alphabet="aB7-", min_size=1, max_size=6), TEXT), min_size=1, max_size=40
)


@given(COLUMN)
def test_a_pattern_shown_is_matched_by_at_least_its_share_of_the_values(values: list[str]) -> None:
    shapes: dict[str, int] = {}
    for value in values:
        shapes[shape_of(value)] = shapes.get(shape_of(value), 0) + 1

    found = best_pattern(list(shapes.items()), len(values))

    if found is not None:
        regex, share = found
        matching = sum(re.fullmatch(regex, value, re.DOTALL) is not None for value in values)
        assert share >= 0.95
        assert matching >= share * len(values) - 1e-9


def test_shapes_become_readable_patterns() -> None:
    assert shape_regex(shape_of("CA-2017-152156")) == "^[A-Z]{2}\\-[0-9]{4}\\-[0-9]{6}$"
    assert shape_regex(shape_of("Claire Gute")) == "^[A-Z][a-z]{5}\\ [A-Z][a-z]{3}$"


def test_a_pattern_needs_95_percent_and_tries_one_shape_first() -> None:
    ids = [(shape_of("AB-12345"), 95), (shape_of("ab-12345"), 5)]
    assert best_pattern(ids, 100) == ("^[A-Z]{2}\\-[0-9]{5}$", 0.95)
    assert best_pattern([(shape_of("AB-12345"), 94), (shape_of("ab-12345"), 6)], 100) is None
    # Names line up once lengths may vary; a run whose length never varies keeps it.
    names = [(shape_of("Claire Gute"), 50), (shape_of("Darin Vanhuff"), 48), ("x", 2)]
    assert best_pattern(names, 100) == ("^[A-Z][a-z]+\\ [A-Z][a-z]+$", 0.98)
    cities = [(shape_of("city 1"), 9), (shape_of("city 10"), 11)]
    assert best_pattern(cities, 20) == ("^[a-z]{4}\\ [0-9]+$", 1.0)
    assert best_pattern([], 0) is None


@pytest.mark.parametrize(
    ("stored", "kind"),
    [
        ("integer", "number"),
        ("bigint", "number"),
        ("numeric(12,4)", "number"),
        ("double precision", "number"),
        ("date", "date"),
        ("timestamp with time zone", "date"),
        ("text", "text"),
        ("character varying(20)", "text"),
        ("boolean", "text"),
        ("jsonb", "other"),
        ("uuid", "other"),
    ],
)
def test_each_stored_type_has_a_kind(stored: str, kind: str) -> None:
    assert kind_of(stored) == kind


# --- a real table ---------------------------------------------------------------------------


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[dict[str, Any]]]:
    url = str(Settings().database_url)  # type: ignore[call-arg]
    with psycopg.connect(url, autocommit=True, row_factory=dict_row) as connection:
        yield connection
        # The tables this file makes are named profile_<random>; leaving them behind would
        # make every later run of the suite add to a pile of them.
        left = connection.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'datasets' "
            "AND tablename LIKE 'profile\\_%'"
        ).fetchall()
        for row in left:
            connection.execute(
                sql.SQL("DROP TABLE {}").format(sql.Identifier("datasets", row["tablename"]))
            )


def _table(conn: psycopg.Connection[dict[str, Any]], definition: str, rows: str) -> str:
    name = f"profile_{uuid4().hex[:10]}"
    conn.execute("CREATE SCHEMA IF NOT EXISTS datasets")
    table = sql.Identifier("datasets", name)
    conn.execute(sql.SQL("CREATE TABLE {} (" + definition + ")").format(table))
    conn.execute(sql.SQL("INSERT INTO {} " + rows).format(table))
    return name


def _columns(conn: psycopg.Connection[dict[str, Any]], name: str) -> list[tuple[str, str]]:
    rows = conn.execute(
        "SELECT attname AS name, format_type(atttypid, atttypmod) AS type FROM pg_attribute "
        "WHERE attrelid = to_regclass(%s) AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
        [f"datasets.{name}"],
    ).fetchall()
    return [(row["name"], row["type"]) for row in rows]


def _by_name(columns: list[ColumnProfile]) -> dict[str, ColumnProfile]:
    return {column.name: column for column in columns}


@pytest.mark.db
def test_a_known_table_is_profiled_exactly(conn: psycopg.Connection[dict[str, Any]]) -> None:
    # 20 rows: i 1..20; ratio has a NaN and a null; code is AB-123 shaped for 19 of 20;
    # colour has 3 values; city has 20 different ones; day spans January 2024.
    name = _table(
        conn,
        "i integer, ratio double precision, price numeric(8,2), day date, code text, "
        "colour text, city text, flag boolean, doc jsonb, _run_id uuid",
        "SELECT i, CASE WHEN i = 3 THEN 'NaN'::float8 WHEN i = 4 THEN NULL ELSE i / 4.0 END, "
        "i * 1.25, date '2024-01-01' + (i - 1), "
        "CASE WHEN i = 20 THEN 'odd one' ELSE 'AB-' || (100 + i) END, "
        "(ARRAY['red', 'red', 'blue', 'green'])[1 + i % 4], 'city ' || i, i % 2 = 0, "
        "NULL, NULL FROM generate_series(1, 20) AS i",
    )

    profile = profile_table(conn, name, _columns(conn, name), json_value)

    assert (profile.table_rows, profile.profiled_rows, profile.sampled) == (20, 20, False)
    columns = _by_name(profile.columns)
    assert "_run_id" not in columns
    i = columns["i"]
    assert (i.kind, i.missing, i.min, i.max, i.mean) == ("number", 0, 1, 20, "10.5000")
    assert i.histogram is not None and len(i.histogram) == HISTOGRAM_BARS
    assert i.histogram == [1] * 20
    ratio = columns["ratio"]
    # The NaN is present, so not missing, but it is left out of the range and the histogram.
    assert (ratio.missing, ratio.min, ratio.max) == (1, 0.25, 5.0)
    assert ratio.histogram is not None and sum(ratio.histogram) == 18
    assert (columns["price"].min, columns["price"].max) == ("1.25", "25.00")
    day = columns["day"]
    assert (day.kind, day.min, day.max, day.mean) == ("date", "2024-01-01", "2024-01-20", None)
    assert day.histogram is not None and sum(day.histogram) == 20
    code = columns["code"]
    assert (code.pattern, code.pattern_share) == ("^[A-Z]{2}\\-[0-9]{3}$", 0.95)
    assert code.distinct == 20 and code.appear_once == 20
    assert [v.value for v in code.least_used or []] == ["AB-101", "AB-102", "AB-103"]
    colour = columns["colour"]
    # Fewer than 6 distinct values: every one is listed, most used first, ties by value.
    assert [(v.value, v.count) for v in colour.all_values or []] == [
        ("red", 10),
        ("blue", 5),
        ("green", 5),
    ]
    assert colour.most_used is None and colour.least_used is None
    city = columns["city"]
    # "city 1" and "city 10" differ only in how many digits they have.
    assert city.pattern == "^[a-z]{4}\\ [0-9]+$"
    assert [(v.value, v.count) for v in city.most_used or []] == [
        ("city 1", 1),
        ("city 10", 1),
        ("city 11", 1),
    ]
    flag = columns["flag"]
    assert [(v.value, v.count) for v in flag.all_values or []] == [("false", 10), ("true", 10)]
    # "true" and "false" share a shape, so a pattern here would say nothing.
    assert flag.pattern is None
    doc = columns["doc"]
    assert (doc.kind, doc.missing, doc.histogram, doc.all_values) == ("other", 20, None, None)


@pytest.mark.db
def test_a_column_of_one_value_and_an_empty_table_are_profiled(
    conn: psycopg.Connection[dict[str, Any]],
) -> None:
    same = _table(conn, "v numeric(4,1)", "SELECT 2.5 FROM generate_series(1, 7)")
    empty = _table(conn, "v integer, t text", "SELECT 1, 'x' WHERE false")

    (only,) = profile_table(conn, same, _columns(conn, same), json_value).columns
    assert (only.min, only.max, only.histogram) == ("2.5", "2.5", [7] + [0] * 19)
    blank = profile_table(conn, empty, _columns(conn, empty), json_value)
    assert blank.table_rows == 0
    number, words = blank.columns
    assert (number.missing, number.min, number.histogram) == (0, None, [0] * 20)
    assert (words.distinct, words.all_values, words.pattern) == (0, [], None)


@pytest.mark.db
def test_a_table_over_the_limit_is_profiled_on_a_sample_of_exactly_the_limit(
    conn: psycopg.Connection[dict[str, Any]],
) -> None:
    name = _table(
        conn,
        "i integer, v double precision",
        "SELECT i, CASE WHEN i % 10 = 0 THEN NULL ELSE i END FROM generate_series(1, 3000) AS i",
    )

    profile = profile_table(conn, name, _columns(conn, name), json_value, row_limit=1000)

    assert (profile.table_rows, profile.profiled_rows, profile.sampled) == (3000, 1000, True)
    i, v = profile.columns
    assert i.missing == 0 and i.histogram is not None and sum(i.histogram) == 1000
    assert v.histogram is not None and sum(v.histogram) + v.missing == 1000
    # A random sample of a whole table, not its first thousand rows.
    assert isinstance(i.max, int) and i.max > 1000
    # The sample is gone once the profile is made.
    assert conn.execute("SELECT to_regclass('pg_temp.profile_sample') AS t").fetchone() == {
        "t": None
    }


@pytest.mark.db
def test_histogram_bars_add_up_to_the_finite_values(
    conn: psycopg.Connection[dict[str, Any]],
) -> None:
    values = [Decimal("-3.5"), Decimal("0"), Decimal("0"), Decimal("7.25"), Decimal("100")]
    name = _table(
        conn,
        "v numeric(6,2), d date",
        "SELECT v, date '2020-02-29' + (v::int * 30) FROM unnest(ARRAY["
        + ", ".join(str(value) for value in values)
        + "]::numeric[]) AS v",
    )

    v, d = profile_table(conn, name, _columns(conn, name), json_value).columns

    assert v.histogram is not None and sum(v.histogram) == 5
    # Bars are 5.175 wide from -3.5, so -3.5 and both zeros share the first; 100 is the last.
    assert v.histogram[0] == 3 and v.histogram[-1] == 1
    assert (v.min, v.max) == ("-3.50", "100.00")
    # -3.5 becomes -4 as an integer (Postgres rounds halves away from zero): 120 days earlier.
    assert d.min == (date(2020, 2, 29) - timedelta(days=120)).isoformat()


@pytest.mark.db
def test_a_huge_value_is_cut_to_a_readable_length(conn: psycopg.Connection[dict[str, Any]]) -> None:
    name = _table(conn, "note text", "SELECT repeat('x', 5000) UNION ALL SELECT 'short'")

    (note,) = profile_table(conn, name, _columns(conn, name), json_value).columns

    assert note.distinct == 2
    shown = sorted(len(str(value.value)) for value in note.all_values or [])
    assert shown == [len("short"), SHOWN_LENGTH]


@pytest.mark.db
def test_a_column_the_database_refuses_does_not_lose_the_other_columns(
    conn: psycopg.Connection[dict[str, Any]],
) -> None:
    # A numeric this large is a fine value to store and too large to cast to float8, so the
    # range query for it fails where the same query for "ok" does not.
    name = _table(conn, "big numeric, ok integer", "SELECT '1e400'::numeric, 7")

    big, ok = profile_table(conn, name, _columns(conn, name), json_value).columns

    assert (big.name, big.kind, big.min, big.histogram) == ("big", "number", None, None)
    assert (ok.min, ok.max) == (7, 7)
