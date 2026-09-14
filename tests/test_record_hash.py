from datetime import UTC, date, datetime, time
from hashlib import sha256
from typing import Any
from uuid import uuid7

import polars as pl
from hypothesis import given
from hypothesis import strategies as st

from udp.pipeline.load import record_hashes, with_platform_columns


def test_golden_hash_is_sha256_of_sorted_json() -> None:
    frame = pl.DataFrame({"name": ["Anna", None], "id": [1, 2], "value": [1.5, float("nan")]})

    hashes = record_hashes(frame).to_list()

    assert hashes == [
        sha256(b'{"id":1,"name":"Anna","value":"1.5"}').hexdigest(),
        sha256(b'{"id":2,"name":null,"value":"NaN"}').hexdigest(),
    ]
    assert hashes[0] == "777edc5ba1d407689d42730dbd4ad94ced775dbc9db411355e95c4f4d408a10d"


def test_golden_hash_pins_the_encoding_of_dates_times_and_booleans() -> None:
    moment = datetime(2020, 1, 1, 1, 2, 3, 456789)
    frame = pl.DataFrame(
        {
            "active": [True],
            "day": [date(2020, 1, 2)],
            "at": [moment],
            "at_utc": [moment.replace(tzinfo=UTC)],
            "clock": [time(1, 2, 3, 4)],
        }
    )

    assert record_hashes(frame).to_list() == [
        sha256(
            b'{"active":true,"at":"2020-01-01 01:02:03.456789",'
            b'"at_utc":"2020-01-01T01:02:03.456789+00:00","clock":"01:02:03.000004",'
            b'"day":"2020-01-02"}'
        ).hexdigest()
    ]


text = st.one_of(
    st.text(max_size=10),
    st.sampled_from(["", "null", "a,b", 'a"b', "a\\b", "1", ","]),
)

COLUMN_KINDS: dict[str, tuple[pl.DataType, st.SearchStrategy[Any]]] = {
    "int": (pl.Int64(), st.integers(-(2**63), 2**63 - 1)),
    "float": (pl.Float64(), st.floats()),
    "bool": (pl.Boolean(), st.booleans()),
    "text": (pl.String(), text),
    "date": (pl.Date(), st.dates()),
    "datetime": (pl.Datetime("us"), st.datetimes()),
}


@st.composite
def frames(draw: st.DrawFn) -> pl.DataFrame:
    kinds = draw(st.lists(st.sampled_from(sorted(COLUMN_KINDS)), min_size=1, max_size=5))
    height = draw(st.integers(0, 20))
    series = []
    for index, kind in enumerate(kinds):
        dtype, values = COLUMN_KINDS[kind]
        column = draw(st.lists(st.none() | values, min_size=height, max_size=height))
        series.append(pl.Series(f"c{index}", column, dtype=dtype))
    return pl.DataFrame(series)


@given(frames(), st.randoms(use_true_random=False), st.integers(1, 7))
def test_hash_ignores_column_order_row_order_and_chunking(
    frame: pl.DataFrame, random: Any, chunk_size: int
) -> None:
    expected = record_hashes(frame).to_list()

    columns = frame.columns[:]
    random.shuffle(columns)
    assert record_hashes(frame.select(columns)).to_list() == expected

    order = list(range(frame.height))
    random.shuffle(order)
    shuffled = frame[order]
    assert record_hashes(shuffled).to_list() == [expected[i] for i in order]

    chunked = [h for chunk in frame.iter_slices(chunk_size) for h in record_hashes(chunk)]
    assert chunked == expected


@given(frames())
def test_hash_ignores_platform_columns(frame: pl.DataFrame) -> None:
    with_platform = with_platform_columns(frame, run_id=uuid7(), loaded_at=datetime.now(UTC))

    assert with_platform["_record_hash"].to_list() == record_hashes(frame).to_list()
    assert record_hashes(with_platform).to_list() == record_hashes(frame).to_list()


@given(st.sampled_from(["a", "a,b", 'a"', "", "null", "1"]), st.sampled_from(["b", "", ",b", "1"]))
def test_moving_a_boundary_changes_the_hash(left: str, right: str) -> None:
    joined = left + right
    for cut in range(len(joined) + 1):
        if joined[:cut] == left:
            continue
        pair = pl.DataFrame({"x": [left, joined[:cut]], "y": [right, joined[cut:]]})
        first, second = record_hashes(pair).to_list()
        assert first != second


def test_null_empty_and_the_word_null_all_differ() -> None:
    frame = pl.DataFrame({"x": [None, "", "null"]}, schema={"x": pl.String})

    assert len(set(record_hashes(frame).to_list())) == 3


def test_nan_infinity_and_null_differ() -> None:
    frame = pl.DataFrame({"x": [None, float("nan"), float("inf"), float("-inf"), -0.0, 0.0]})

    assert len(set(record_hashes(frame).to_list())) == 6


def test_same_text_in_different_columns_differs() -> None:
    frame = pl.DataFrame({"a": ["1", None], "b": [None, "1"]})

    first, second = record_hashes(frame).to_list()
    assert first != second
