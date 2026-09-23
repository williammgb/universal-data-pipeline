"""A column-by-column profile of one dataset table, worked out when it is asked for.

Numbers and dates get their range and a histogram, text gets its most and least used values
and, when nearly every value has the same shape, that shape as a regular expression. A table
over the row limit is profiled on a random sample of exactly that many rows.
"""

import re
from collections.abc import Callable, Sequence
from itertools import groupby
from typing import Any

import psycopg
import structlog
from psycopg import sql

from udp.api.models import ColumnProfile, DatasetProfile, JsonValue, ProfileKind, ValueCount
from udp.names import RESERVED_COLUMNS

PROFILE_ROW_LIMIT = 1_000_000
HISTOGRAM_BARS = 20
# Below this many distinct values every value is listed, so none is both most and least used.
LIST_ALL_BELOW = 6
SHOWN_VALUES = 3
# A shown value is cut to this many characters: a profile is a summary, and one column can
# hold a pasted document or a base64 blob.
SHOWN_LENGTH = 200
PATTERN_SHARE = 0.95
# More shapes than this and no single one can be the pattern worth showing.
MAX_SHAPES = 5000
SAMPLE_TABLE = "profile_sample"

_NUMBER_TYPES = {"smallint", "integer", "bigint", "real", "double precision"}
_NON_FINITE = {"real", "double precision"}
_CLASSES = {"A": "[A-Z]", "a": "[a-z]", "9": "[0-9]"}

log = structlog.get_logger()

Connection = psycopg.Connection[dict[str, Any]]
# How a stored value becomes JSON; the catalog's own rule, passed in so both agree.
ToJson = Callable[[Any], JsonValue]


def kind_of(stored_type: str) -> ProfileKind:
    """What the profile shows for a column, from its stored type as format_type writes it."""
    if stored_type in _NUMBER_TYPES or stored_type.startswith("numeric"):
        return "number"
    if stored_type == "date" or stored_type.startswith("timestamp"):
        return "date"
    if stored_type == "text" or stored_type.startswith("character") or stored_type == "boolean":
        return "text"
    return "other"


def shape_of(value: str) -> str:
    """Every ASCII digit as 9, capital as A and small letter as a; anything else kept."""
    value = re.sub(r"[0-9]", "9", value)
    value = re.sub(r"[A-Z]", "A", value)
    return re.sub(r"[a-z]", "a", value)


def runs_of(shape: str) -> list[tuple[str, int]]:
    """A shape as its runs: "AA-999" is A twice, - once, 9 three times."""
    return [(char, len(list(run))) for char, run in groupby(shape)]


def runs_regex(chars: Sequence[str], lengths: Sequence[set[int]]) -> str:
    """The regular expression for runs of these characters: a run whose length never varies
    keeps it exactly, and one that does may be any length."""
    parts = []
    for char, seen in zip(chars, lengths, strict=True):
        token = _CLASSES.get(char, re.escape(char))
        if seen == {1}:
            parts.append(token)
        elif len(seen) == 1:
            parts.append(f"{token}{{{next(iter(seen))}}}")
        else:
            parts.append(token + "+")
    return "^" + "".join(parts) + "$"


def shape_regex(shape: str) -> str:
    """The regular expression that matches exactly the values of this one shape."""
    runs = runs_of(shape)
    return runs_regex([char for char, _ in runs], [{length} for _, length in runs])


def best_pattern(shapes: list[tuple[str, int]], present: int) -> tuple[str, float] | None:
    """The pattern at least PATTERN_SHARE of the present values match: one shape if that is
    enough, otherwise the shapes whose runs line up once their lengths are allowed to vary
    ("city 1" and "city 10")."""
    if present == 0 or not shapes:
        return None
    shape, count = max(shapes, key=lambda item: (item[1], item[0]))
    if count / present >= PATTERN_SHARE:
        return shape_regex(shape), count / present
    groups: dict[tuple[str, ...], tuple[int, list[set[int]]]] = {}
    for shape, count in shapes:
        runs = runs_of(shape)
        chars = tuple(char for char, _ in runs)
        total, lengths = groups.get(chars, (0, [set() for _ in runs]))
        for seen, (_, length) in zip(lengths, runs, strict=True):
            seen.add(length)
        groups[chars] = (total + count, lengths)
    chars, (count, lengths) = max(groups.items(), key=lambda item: (item[1][0], item[0]))
    if count / present >= PATTERN_SHARE:
        return runs_regex(chars, lengths), count / present
    return None


def _finite(name: str, stored_type: str, kind: ProfileKind) -> sql.Composable:
    column = sql.Identifier(name)
    if kind == "date":
        return sql.SQL("isfinite({})").format(column)
    if stored_type in _NON_FINITE or stored_type.startswith("numeric"):
        return sql.SQL("{} NOT IN ('NaN', 'Infinity', '-Infinity')").format(column)
    return sql.SQL("true")


def _as_number(name: str, kind: ProfileKind) -> sql.Composable:
    column = sql.Identifier(name)
    if kind == "date":
        return sql.SQL("extract(epoch FROM {})::float8").format(column)
    return sql.SQL("{}::float8").format(column)


def profile_table(
    conn: Connection,
    table: str,
    columns: list[tuple[str, str]],
    json_value: ToJson,
    row_limit: int = PROFILE_ROW_LIMIT,
) -> DatasetProfile:
    """Profile datasets.<table>; columns are (name, stored type) pairs in table order."""
    target = sql.Identifier("datasets", table)
    shown = [(name, kind) for name, kind in columns if name not in RESERVED_COLUMNS]
    with conn.transaction():
        row = conn.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(target)).fetchone()
        table_rows = int(row["n"]) if row else 0
        source: sql.Composable = target
        sampled = table_rows > row_limit
        if sampled:
            # A little over the needed share, then row_limit of those in random order: the
            # sample is uniform, and at most the stated size — an unlucky Bernoulli draw can
            # leave it a little short, which is why profiled_rows is counted rather than assumed.
            percent = min(100.0, row_limit / table_rows * 110)
            conn.execute(
                sql.SQL(
                    "CREATE TEMP TABLE {} ON COMMIT DROP AS SELECT * FROM {} "
                    "TABLESAMPLE BERNOULLI ({}) ORDER BY random() LIMIT {}"
                ).format(
                    sql.Identifier(SAMPLE_TABLE),
                    target,
                    sql.Literal(percent),
                    sql.Literal(row_limit),
                )
            )
            source = sql.Identifier(SAMPLE_TABLE)
            row = conn.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(source)).fetchone()
            profiled = int(row["n"]) if row else 0
        else:
            profiled = table_rows
        profiles = [
            _column_or_bare(conn, source, name, stored_type, profiled, json_value)
            for name, stored_type in shown
        ]
    return DatasetProfile(
        table_rows=table_rows, profiled_rows=profiled, sampled=sampled, columns=profiles
    )


def _column_or_bare(
    conn: Connection,
    source: sql.Composable,
    name: str,
    stored_type: str,
    rows: int,
    json_value: ToJson,
) -> ColumnProfile:
    """One column's profile, or a bare entry naming it when the database refuses the column —
    a value too large to cast to float8, for one. Each column gets its own savepoint, so one
    such column cannot take the whole dataset's profile down with it."""
    try:
        with conn.transaction():
            return _profile_column(conn, source, name, stored_type, rows, json_value)
    except psycopg.Error as error:
        log.warning("column could not be profiled", column=name, type=stored_type, error=str(error))
        return ColumnProfile(name=name, type=stored_type, kind=kind_of(stored_type), missing=0)


def _profile_column(
    conn: Connection,
    source: sql.Composable,
    name: str,
    stored_type: str,
    rows: int,
    json_value: ToJson,
) -> ColumnProfile:
    kind = kind_of(stored_type)
    column = sql.Identifier(name)
    present_row = conn.execute(
        sql.SQL("SELECT count({}) AS n FROM {}").format(column, source)
    ).fetchone()
    present = int(present_row["n"]) if present_row else 0
    profile = ColumnProfile(name=name, type=stored_type, kind=kind, missing=rows - present)
    if kind in ("number", "date"):
        _add_range(conn, source, name, stored_type, kind, profile, json_value)
    elif kind == "text":
        _add_values(conn, source, name, stored_type, present, profile)
    return profile


def _add_range(
    conn: Connection,
    source: sql.Composable,
    name: str,
    stored_type: str,
    kind: ProfileKind,
    profile: ColumnProfile,
    json_value: ToJson,
) -> None:
    column = sql.Identifier(name)
    finite = _finite(name, stored_type, kind)
    mean = (
        sql.SQL("round(avg({})::numeric, 4)").format(column)
        if kind == "number"
        else sql.SQL("NULL")
    )
    number = _as_number(name, kind)
    found = conn.execute(
        sql.SQL(
            "SELECT min({c}) AS lo, max({c}) AS hi, {mean} AS mean, count({c}) AS n, "
            "min({x}) AS x_lo, max({x}) AS x_hi FROM {s} WHERE {c} IS NOT NULL AND {f}"
        ).format(c=column, mean=mean, x=number, s=source, f=finite)
    ).fetchone()
    if found is None or not found["n"]:
        profile.histogram = [0] * HISTOGRAM_BARS
        return
    profile.min, profile.max = json_value(found["lo"]), json_value(found["hi"])
    profile.mean = json_value(found["mean"])
    low, high, count = found["x_lo"], found["x_hi"], int(found["n"])
    if low == high:
        profile.histogram = [count] + [0] * (HISTOGRAM_BARS - 1)
        return
    bars = conn.execute(
        sql.SQL(
            # A value outside the range this query's own min and max came from — a row loaded
            # between the two queries — lands in bucket 0 or 21; it belongs in the end bar.
            "SELECT greatest(least(width_bucket({x}, %s, %s, %s), %s), 1) AS bar, count(*) AS n "
            "FROM {s} WHERE {c} IS NOT NULL AND {f} GROUP BY 1"
        ).format(x=number, s=source, c=column, f=finite),
        [low, high, HISTOGRAM_BARS, HISTOGRAM_BARS],
    ).fetchall()
    histogram = [0] * HISTOGRAM_BARS
    for bar in bars:
        histogram[int(bar["bar"]) - 1] += int(bar["n"])
    profile.histogram = histogram


def _add_values(
    conn: Connection,
    source: sql.Composable,
    name: str,
    stored_type: str,
    present: int,
    profile: ColumnProfile,
) -> None:
    column = sql.Identifier(name)
    found = conn.execute(
        sql.SQL(
            "WITH g AS (SELECT {c}::text AS v, count(*) AS n FROM {s} "
            "WHERE {c} IS NOT NULL GROUP BY 1) "
            "SELECT (SELECT count(*) FROM g) AS distinct_values, "
            "(SELECT count(*) FROM g WHERE n = 1) AS once, "
            "(SELECT coalesce(json_agg(json_build_array(left(v, %s), n)), '[]') FROM "
            "(SELECT v, n FROM g ORDER BY n DESC, v LIMIT %s) AS top) AS top, "
            "(SELECT coalesce(json_agg(json_build_array(left(v, %s), n)), '[]') FROM "
            "(SELECT v, n FROM g ORDER BY n, v LIMIT %s) AS bottom) AS bottom"
        ).format(c=column, s=source),
        [SHOWN_LENGTH, LIST_ALL_BELOW - 1, SHOWN_LENGTH, SHOWN_VALUES],
    ).fetchone()
    if found is None:
        return
    top = [ValueCount(value=v, count=n) for v, n in found["top"]]
    profile.distinct = int(found["distinct_values"])
    profile.appear_once = int(found["once"])
    if profile.distinct < LIST_ALL_BELOW:
        profile.all_values = top
    else:
        profile.most_used = top[:SHOWN_VALUES]
        profile.least_used = [ValueCount(value=v, count=n) for v, n in found["bottom"]]
    if stored_type == "boolean":
        # "true" and "false" share a shape, so every boolean column would show ^[a-z]+$.
        return
    shapes = conn.execute(
        sql.SQL(
            "SELECT regexp_replace(regexp_replace(regexp_replace({c}::text, "
            "'[0-9]', '9', 'g'), '[A-Z]', 'A', 'g'), '[a-z]', 'a', 'g') AS shape, "
            "count(*) AS n FROM {s} WHERE {c} IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT %s"
        ).format(c=column, s=source),
        [MAX_SHAPES + 1],
    ).fetchall()
    if len(shapes) > MAX_SHAPES:
        return
    pattern = best_pattern([(row["shape"], int(row["n"])) for row in shapes], present)
    if pattern is not None:
        profile.pattern, profile.pattern_share = pattern
