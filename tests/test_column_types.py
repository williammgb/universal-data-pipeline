import json
import math
import re
from datetime import UTC, date, datetime, time
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation, localcontext
from itertools import pairwise
from typing import Any

import polars as pl
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from polars.testing import assert_frame_equal
from structlog.testing import capture_logs

from udp.errors import ValidationError
from udp.pipeline.column_types import apply_column_types, convert
from udp.storage.loader import RunFindings

# --- a model of the conversion table, written with Python's own parsers ---------------------

BAD = object()
DECIMAL = "decimal(6,2)"
DECLARED = ["text", "integer", DECIMAL, "float", "boolean", "date", "timestamp", "json"]

NUMBER = re.compile(r"[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?")
FLOAT_WORD = re.compile(r"[+-]?(nan|inf|infinity)", re.IGNORECASE | re.ASCII)
DATE_TEXT = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
TIMESTAMP_TEXT = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}"
    r"([T ][0-9]{2}:[0-9]{2}(:[0-9]{2}(\.[0-9]{1,6})?)?(Z|[+-][0-9]{2}:[0-9]{2})?)?"
)
OFFSET_MINUTES = re.compile(r"[+-][0-9]{2}:[0-9]{2}$")


def _decimal_model(value: Decimal, precision: int, scale: int) -> Any:
    with localcontext(prec=400, Emax=999_999_999, Emin=-999_999_999):
        try:
            rounded = value.quantize(Decimal(10) ** -scale)
        except InvalidOperation:
            return BAD
        if rounded != value or abs(value) >= Decimal(10) ** (precision - scale):
            return BAD
    return value


def _utc(moment: datetime) -> Any:
    try:
        converted = moment.replace(tzinfo=UTC) if moment.tzinfo is None else moment.astimezone(UTC)
    except OverflowError:
        return BAD
    return converted if 1 <= converted.year <= 9999 else BAD


def model(value: Any, source: str, declared: str) -> Any:
    """What a non-null value becomes, or BAD when its row must go to quarantine."""
    if declared == "text":
        return value  # compared by round trip in _same
    if declared == "integer":
        if source == "text":
            ok = re.fullmatch(r"[+-]?[0-9]+", value) and -(2**63) <= int(value) < 2**63
            return int(value) if ok else BAD
        if source == "float":
            whole = math.isfinite(value) and value.is_integer()
            return int(value) if whole and -(2.0**63) <= value < 2.0**63 else BAD
        if source == "decimal":
            return int(value) if value == value.to_integral_value() else BAD
        return value
    if declared == DECIMAL:
        if source == "text":
            return _decimal_model(Decimal(value), 6, 2) if NUMBER.fullmatch(value) else BAD
        if source == "float":
            # A float's extra digits are how it was stored, so it is rounded half away from
            # zero to the scale first; text and exact decimals are never rounded.
            if not math.isfinite(value):
                return BAD
            with localcontext(prec=400, Emax=999_999_999, Emin=-999_999_999):
                rounded = Decimal(repr(value)).quantize(Decimal("0.01"), ROUND_HALF_UP)
            return _decimal_model(rounded, 6, 2)
        return _decimal_model(Decimal(value), 6, 2)
    if declared == "float":
        if source == "text":
            if FLOAT_WORD.fullmatch(value):
                return float(value)
            if NUMBER.fullmatch(value) and math.isfinite(float(value)):
                return float(value)
            return BAD
        if source == "int":
            return float(value) if abs(value) <= 2**53 else BAD
        return float(value)
    if declared == "boolean":
        if source == "text":
            lower = value.lower() if value.isascii() else None
            return {"true": True, "t": True, "yes": True, "y": True, "1": True}.get(
                lower or "",
                {"false": False, "f": False, "no": False, "n": False, "0": False}.get(
                    lower or "", BAD
                ),
            )
        if source == "int":
            return {1: True, 0: False}.get(value, BAD)
        return value
    if declared == "date":
        if source == "text":
            if not DATE_TEXT.fullmatch(value):
                return BAD
            try:
                return date.fromisoformat(value)
            except ValueError:
                return BAD
        if source in ("naive", "utc"):
            moment = value.astimezone(UTC) if value.tzinfo else value
            return moment.date() if moment.time() == time() else BAD
        return value
    if declared == "timestamp":
        if source == "text":
            # Python reads 24:00 as the next midnight, and an offset of +00:60 as the next
            # hour; the rules allow hours 00 to 23 and offset minutes 00 to 59 only.
            if not TIMESTAMP_TEXT.fullmatch(value) or value[11:13] == "24":
                return BAD
            if OFFSET_MINUTES.search(value) and int(value[-2:]) > 59:
                return BAD
            try:
                return _utc(datetime.fromisoformat(value))
            except ValueError:
                return BAD
        if source == "date":
            return datetime.combine(value, time(), UTC)
        return _utc(value)
    # json
    if source == "text":

        def refuse(constant: str) -> Any:
            raise ValueError(constant)

        try:
            json.loads(value, parse_constant=refuse)
        except ValueError:
            return BAD
        return value
    if source == "float" and not math.isfinite(value):
        return BAD
    return value


def _same(result: Any, expected: Any, source: str, declared: str) -> bool:
    if declared == "text":
        if source == "float":
            if math.isnan(float(result)):
                return math.isnan(expected)
            return bool(float(result) == expected)
        if source == "decimal":
            return bool(Decimal(result) == expected)
        if source == "bool":
            return bool(result == ("true" if expected else "false"))
        if source in ("date", "naive", "utc"):
            return bool(type(expected).fromisoformat(result) == expected)
        if source == "int":
            return bool(int(result) == expected)
        return bool(result == expected)
    if declared == "json" and source != "text":
        return bool(json.loads(result) == expected)
    if isinstance(expected, float) and math.isnan(expected):
        return isinstance(result, float) and math.isnan(result)
    return bool(result == expected and type(result) is type(expected))


# --- generators ------------------------------------------------------------------------------

ARABIC_DIGITS = chr(0x661) + chr(0x662)
FULL_WIDTH_DIGITS = chr(0xFF11) + chr(0xFF12)

TEXT_PIECES = [
    "", "0", "00", "5", "12", "9999", "10000", "123456", ".", "e", "E", "-", "+", "_", ",", " ",
    ARABIC_DIGITS, FULL_WIDTH_DIGITS, "NaN", "nan", "Infinity", "-inf", "1e999",
    "2026-02-30", "2024-02-29", "0000-01-01", "T", ":", "23:59:60", "10:00", "Z", "+05:30",
    "+0530", "+25:00", ".1234567", ".5", "TRUE", "Yes", "2", "{", '"x"', "true", "null",
]  # fmt: skip
TEXT_VALUES = [
    "12.34", "12.345", "9999.99", "10000.00", "-0.00", "0012.10", "1E-7", "1e3", "5.", ".5",
    "+-1", "1_000", "1,5", "1 000", "9223372036854775807", "9223372036854775808",
    "-9223372036854775808", "9007199254740993", "1.5e2", "0e5", "2024-01-01", "2024-01-01 10:00",
    "2024-01-01T10:00:00.123456+02:00", "2024-01-01T10:00:00Z", "20260913T101010",
    "2024-01-01T10:00+23:59", "0001-01-01T00:30+01:00", "9999-12-31T23:30-01:00",
    '{"a": [1, 2.5]}', "NaN", "[1,", "y", "N", "t", "F",
]  # fmt: skip

text_value = st.one_of(
    st.sampled_from(TEXT_VALUES),
    st.lists(st.sampled_from(TEXT_PIECES), min_size=1, max_size=4).map("".join),
    st.text(max_size=6),
)

SOURCES: dict[str, tuple[pl.DataType, st.SearchStrategy[Any]]] = {
    "text": (pl.String(), text_value),
    "int": (
        pl.Int64(),
        st.one_of(
            st.integers(-(2**63), 2**63 - 1),
            st.sampled_from([0, 1, 2, -1, 2**53, 2**53 + 1, -(2**53) - 1, 9999, 10000]),
        ),
    ),
    "float": (
        pl.Float64(),
        st.one_of(
            st.floats(allow_nan=True),
            # The last five are spreadsheet-style: a stored tail, a half to round up, a round
            # up past the precision, a negative half that must not round toward zero, and
            # a value that rounds to zero.
            st.sampled_from(
                [
                    0.5,
                    12.34,
                    12.345,
                    1e20,
                    2.0**63,
                    -(2.0**63),
                    9999.99,
                    1e-7,
                    3.0,
                    731.9399999999999,
                    2.675,
                    9999.995,
                    -0.005,
                    1e-3,
                ]
            ),
        ),
    ),
    "decimal": (
        pl.Decimal(12, 4),
        st.decimals(min_value=-(10**8) + 1, max_value=10**8 - 1, places=4),
    ),
    "bool": (pl.Boolean(), st.booleans()),
    "date": (pl.Date(), st.dates()),
    "naive": (
        pl.Datetime("us"),
        st.one_of(st.datetimes(), st.dates().map(lambda d: datetime.combine(d, time()))),
    ),
    "utc": (pl.Datetime("us", "UTC"), st.datetimes(timezones=st.just(UTC))),
    "empty": (pl.Null(), st.none()),
}

READABLE = {
    "text": list(SOURCES),
    "integer": ["text", "int", "float", "decimal", "empty"],
    DECIMAL: ["text", "int", "float", "decimal", "empty"],
    "float": ["text", "int", "float", "decimal", "empty"],
    "boolean": ["text", "int", "bool", "empty"],
    "date": ["text", "date", "naive", "utc", "empty"],
    "timestamp": ["text", "date", "naive", "utc", "empty"],
    "json": ["text", "int", "float", "bool", "empty"],
}


PAIRS = [(declared, source) for declared in DECLARED for source in READABLE[declared]]


@st.composite
def declared_columns(
    draw: st.DrawFn, declared: str, source: str
) -> tuple[str, str, list[pl.DataFrame]]:
    dtype, strategy = SOURCES[source]
    values = draw(st.lists(st.one_of(strategy, st.none()), max_size=12))
    frame = pl.DataFrame(
        [pl.Series("id", range(len(values)), dtype=pl.Int64), pl.Series("v", values, dtype=dtype)]
    )
    cuts = sorted(draw(st.lists(st.integers(0, len(values)), max_size=3)))
    chunks = [frame[start:end] for start, end in pairwise([0, *cuts, len(values)])]
    return declared, source, chunks


def _run(chunks: list[pl.DataFrame], declared: str) -> tuple[pl.DataFrame, RunFindings]:
    findings = RunFindings("shop", "orders")
    kept = list(apply_column_types(iter(chunks), {"v": declared}, findings))
    return pl.concat(kept), findings


@pytest.mark.parametrize(("declared", "source"), PAIRS)
@settings(max_examples=settings().max_examples // 4)  # 36 pairs, each with its own examples
@given(data=st.data())
def test_each_value_is_converted_exactly_or_its_row_quarantined(
    declared: str, source: str, data: st.DataObject
) -> None:
    _, _, chunks = data.draw(declared_columns(declared, source))
    whole = pl.concat(chunks)

    kept, _ = _run(chunks, declared)

    kept_values = dict(zip(kept["id"].to_list(), kept["v"].to_list(), strict=True))
    for row_id, value in zip(whole["id"].to_list(), whole["v"].to_list(), strict=True):
        expected = None if value is None else model(value, source, declared)
        if expected is BAD:
            assert row_id not in kept_values, (value, kept_values.get(row_id))
        else:
            assert row_id in kept_values, value
            result = kept_values[row_id]
            if expected is None:
                assert result is None
            else:
                assert _same(result, expected, source, declared), (value, result, expected)


@given(st.sampled_from(PAIRS).flatmap(lambda pair: declared_columns(*pair)))
def test_kept_and_quarantined_rows_add_up_and_chunks_do_not_matter(
    generated: tuple[str, str, list[pl.DataFrame]],
) -> None:
    declared, _, chunks = generated
    whole = pl.concat(chunks)

    kept, findings = _run(chunks, declared)
    whole_kept, whole_findings = _run([whole], declared)

    assert kept.height + findings.quarantined_rows == whole.height
    assert_frame_equal(kept, whole_kept)
    records = [row for frame in findings.quarantine for row in frame.rows()]
    assert records == [row for frame in whole_findings.quarantine for row in frame.rows()]
    assert kept.schema["v"] == convert(whole["v"].head(0), declared).dtype


def test_a_bad_row_is_quarantined_as_read_with_every_unfit_column_named() -> None:
    frame = pl.DataFrame(
        {
            "id": ["1", "2", "3"],
            "amount": ["12.34", "12.345", "x"],
            "day": ["2024-01-01", "2026-02-30", "2024-01-03"],
        }
    )
    findings = RunFindings("shop", "orders")

    (kept,) = apply_column_types(
        iter([frame]), {"id": "integer", "amount": DECIMAL, "day": "date"}, findings
    )

    assert kept.rows() == [(1, Decimal("12.34"), date(2024, 1, 1))]
    assert findings.quarantined_rows == 2
    (quarantined,) = findings.quarantine
    assert quarantined["reason"].to_list() == [
        "column 'amount' is not decimal(6,2); column 'day' is not date",
        "column 'amount' is not decimal(6,2)",
    ]
    assert json.loads(quarantined["record"][1]) == {"id": "3", "amount": "x", "day": "2024-01-03"}


def test_floats_round_half_away_from_zero_to_the_scale_and_text_never_does() -> None:
    # 1234567.891 has more decimals than the scale, so it is on the rounding path, and more
    # digits than the precision, so it is refused before being rounded rather than after.
    floats = pl.Series("v", [731.9399999999999, 2.675, -0.005, 9999.995, 12.3, 1234567.891])
    texts = pl.Series("v", ["731.9399999999999", "2.675", "12.345", "12.3"])

    assert convert(floats, DECIMAL).to_list() == [
        Decimal("731.94"),
        Decimal("2.68"),
        Decimal("-0.01"),
        None,  # 10000.00 does not fit decimal(6,2)
        Decimal("12.30"),
        None,
    ]
    assert convert(texts, DECIMAL).to_list() == [None, None, None, Decimal("12.30")]


@pytest.mark.parametrize(
    ("column", "declared", "message"),
    [
        (
            pl.Series("v", [date(2024, 1, 1)]),
            "integer",
            "read as Date and cannot be stored as integer",
        ),
        (pl.Series("v", [1.5]), "boolean", "read as Float64 and cannot be stored as boolean"),
        (pl.Series("v", [True]), "timestamp", "read as Boolean and cannot be stored as timestamp"),
    ],
)
def test_a_column_that_can_never_convert_fails_the_run(
    column: pl.Series, declared: str, message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        list(apply_column_types(iter([column.to_frame()]), {"v": declared}, RunFindings("s", "d")))


def test_a_declared_column_missing_from_the_data_fails_the_run() -> None:
    with pytest.raises(ValidationError, match="declared column 'amount' is not in the data"):
        list(
            apply_column_types(
                iter([pl.DataFrame({"id": [1]})]), {"amount": DECIMAL}, RunFindings("s", "d")
            )
        )


def test_timestamps_become_utc_whatever_their_offset() -> None:
    column = pl.Series(
        "v",
        ["2024-01-01 10:00:05.5+02:00", "2024-01-01", "2024-01-01T23:59:60", "2024-06-01T10:00Z"],
    )

    assert convert(column, "timestamp").to_list() == [
        datetime(2024, 1, 1, 8, 0, 5, 500000, UTC),
        datetime(2024, 1, 1, tzinfo=UTC),
        None,
        datetime(2024, 6, 1, 10, tzinfo=UTC),
    ]
    assert convert(pl.Series("v", [datetime(2024, 1, 1, 12)]), "timestamp").to_list() == [
        datetime(2024, 1, 1, 12, tzinfo=UTC)
    ]


# Every near miss the rules decide on, checked against the model one by one, because random
# draws reach any single one of them only rarely.
NEAR_MISSES = {
    "integer": [
        "+-1", "1_000", "1,5", "1 000", ARABIC_DIGITS, FULL_WIDTH_DIGITS, "1e3", "1.0", "+5",
        "007", "-0", "", "-", "+", "9223372036854775807", "9223372036854775808",
        "-9223372036854775808", "-9223372036854775809",
    ],
    DECIMAL: [
        ".", "5.", ".5", "1.2.3", "9999.99", "10000.00", "10000", "-9999.995", "12.345",
        "0012.10", "-0.00", "1E-7", "1e3", "1e4", "1.5e2", "0e5", "1e999", "1e-999", "+-1",
        "1_000", ARABIC_DIGITS, "NaN", "Infinity", "e5", "",
        # An exponent whose digits carry trailing zeros, or land exactly on the precision or
        # the scale: writing the number out as plain digits has to move the point by exactly
        # as many places as it drops zeros.
        "1.00e2", "1.005e2", "11e0", "9999.99e0",
    ],
    "float": [
        "nan", "+NaN", "-inf", "Infinity", "infinit", "1e999", "-1e999", "1e-999", ".5", "5.",
        "+3", "1_000", "0x10", FULL_WIDTH_DIGITS, "1e5", "e5", ".", "",
    ],
    "boolean": ["TRUE", "Yes", "y", "N", "t", "F", "1", "0", "2", "yes!", "", "tru", "01"],
    "date": [
        "2024-02-29", "2023-02-29", "2026-02-30", "0000-01-01", "2024-1-01", "2024-01-1",
        "20240101", "2024-01-01T00:00", "2024/01/01", "9999-12-31", "0001-01-01",
    ],
    "timestamp": [
        "2024-01-01", "2024-01-01T10:00", "2024-01-01 10:00", "2024-01-01T10",
        "2024-01-01T23:59:60", "2024-01-01T24:00", "2024-01-01T10:00:00.1234567",
        "2024-01-01T10:00:00.123456",
        "2024-01-01T10:00+0530", "2024-01-01T10:00+05:30", "2024-01-01T10:00+25:00",
        "2024-01-01T10:00+23:59", "20260913T101010", "2024-01-01T10:00:00Z", "2024-01-01Z",
        "0001-01-01T00:30+01:00", "9999-12-31T23:30-01:00", "2024-01-01t10:00", "0000-01-01",
        # The first and last moment the rules allow, and the offsets one step past the largest
        # allowed one, which no time zone uses but a file can still hold.
        "0001-01-01", "9999-12-31T23:00:00", "2024-01-01T10:00+24:00", "2024-01-01T10:00+00:60",
    ],
    "json": ['{"a":1}', "NaN", "Infinity", "-Infinity", "{", '"x"', "[1,]", "nul", "null", "1e999"],
}  # fmt: skip


def test_the_declared_columns_are_checked_once_for_the_whole_run_not_once_per_chunk() -> None:
    chunks = [pl.DataFrame({"v": [str(number)]}) for number in range(3)]
    findings = RunFindings("shop", "orders")

    with capture_logs() as logs:
        kept = list(apply_column_types(iter(chunks), {"v": "integer"}, findings))

    assert [frame["v"].to_list() for frame in kept] == [[0], [1], [2]]
    declared = [line for line in logs if line["event"] == "columns declared"]
    assert declared == [
        {
            "event": "columns declared",
            "log_level": "info",
            "step": "columns",
            "columns": {"v": "integer"},
        }
    ]


def test_a_timestamp_becomes_a_date_only_when_its_utc_time_is_midnight() -> None:
    column = pl.Series(
        "v",
        [
            datetime(2024, 1, 1),
            datetime(2024, 1, 1, 0, 0, 1),
            datetime(2024, 1, 1, 0, 1),
            datetime(2024, 1, 1, 1),
            datetime(2024, 1, 1, 0, 0, 0, 1),
        ],
    )

    assert convert(column, "date").to_list() == [date(2024, 1, 1), None, None, None, None]


def test_a_float_exactly_on_the_integer_limit_converts_and_one_past_it_does_not() -> None:
    # -2^63 is the smallest integer that can be stored, and it is a float exactly; 2^63 is one
    # past the largest, because the largest is 2^63 - 1.
    column = pl.Series("v", [-(2.0**63), 2.0**63, 2.0**63 - 1024.0])

    assert convert(column, "integer").to_list() == [-(2**63), None, int(2.0**63 - 1024.0)]


def test_a_whole_number_column_is_a_boolean_only_at_one_and_zero() -> None:
    column = pl.Series("v", [1, 0, 2, -1, 10])

    assert convert(column, "boolean").to_list() == [True, False, None, None, None]


def test_a_whole_number_becomes_a_float_only_while_every_number_still_has_its_own_float() -> None:
    # Past 2^53 the floats run out: 2^53 and 2^53 + 1 would both be stored as the same value,
    # so anything beyond the limit is quarantined rather than quietly rounded.
    column = pl.Series("v", [2**53, 2**53 + 1, -(2**53), -(2**53) - 1, 0])

    assert convert(column, "float").to_list() == [float(2**53), None, float(-(2**53)), None, 0.0]


def test_a_nanosecond_column_keeps_whole_microseconds_and_quarantines_the_rest() -> None:
    # Timestamps are stored to the microsecond, so a nanosecond column may hold a value that
    # cannot be stored without losing digits. Built from whole nanoseconds, because that is the
    # only way to write a value between two microseconds.
    second = int(datetime(2024, 1, 1, 10, tzinfo=UTC).timestamp()) * 1_000_000_000
    column = (
        pl.Series("v", [second, second + 123_456_000, second + 123_456_500], dtype=pl.Int64)
        .cast(pl.Datetime("ns"))
        .alias("v")
    )

    assert convert(column, "timestamp").to_list() == [
        datetime(2024, 1, 1, 10, tzinfo=UTC),
        datetime(2024, 1, 1, 10, 0, 0, 123456, tzinfo=UTC),
        None,  # 500 nanoseconds past a microsecond
    ]


@pytest.mark.parametrize(
    ("declared", "text"),
    [(declared, text) for declared, texts in NEAR_MISSES.items() for text in texts],
)
def test_each_near_miss_follows_the_rules(declared: str, text: str) -> None:
    (result,) = convert(pl.Series("v", [text]), declared).to_list()

    expected = model(text, "text", declared)

    if expected is BAD:
        assert result is None
    else:
        assert result is not None and _same(result, expected, "text", declared), result
