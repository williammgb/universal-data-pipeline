import json
import traceback
from datetime import UTC
from itertools import pairwise
from pathlib import Path
from typing import Any
from uuid import uuid7

import polars as pl
import pytest
from fakes import MemoryLoader
from hypothesis import given
from hypothesis import strategies as st
from polars.testing import assert_frame_equal

from udp.config.source import load_source
from udp.errors import TransformError
from udp.log import configure_logging
from udp.names import RESERVED_COLUMNS
from udp.pipeline.custom import (
    TransformContext,
    TransformFile,
    apply_transform,
    find_transform,
    load_transform,
)
from udp.pipeline.runner import run_source
from udp.pipeline.transform import transform

# --- common transforms: trimmed text, empty text as null ------------------------------------

# Unicode White_Space, which is what Polars trims, written as code points so no invisible
# character sits in the source. FILE SEPARATOR (Python's str.strip removes it) and
# ZERO WIDTH SPACE are near misses that must stay.
WHITE_SPACE = "".join(
    chr(code)
    for code in [
        *range(0x09, 0x0E),
        0x20,
        0x85,
        0xA0,
        0x1680,
        *range(0x2000, 0x200B),
        0x2028,
        0x2029,
        0x202F,
        0x205F,
        0x3000,
    ]
)
FILE_SEPARATOR = chr(0x1C)
ZERO_WIDTH_SPACE = chr(0x200B)
NO_BREAK_SPACE = chr(0xA0)
EM_SPACE = chr(0x2003)
IDEOGRAPHIC_SPACE = chr(0x3000)

text_piece = st.sampled_from(
    [
        "",
        " ",
        "  ",
        "\t",
        "\n",
        "\r",
        NO_BREAK_SPACE,
        EM_SPACE,
        IDEOGRAPHIC_SPACE,
        FILE_SEPARATOR,
        ZERO_WIDTH_SPACE,
        "a",
        "Anna",
        chr(0xE9),
        chr(0x4E2D),
        "x y",
    ]
)
text_value = st.one_of(
    st.none(),
    st.lists(st.one_of(text_piece, st.sampled_from(WHITE_SPACE)), max_size=6).map("".join),
    st.text(max_size=8),
)

VALUES: dict[str, tuple[pl.DataType, st.SearchStrategy[Any]]] = {
    "text": (pl.String(), text_value),
    "int": (pl.Int64(), st.one_of(st.none(), st.integers(-(2**63), 2**63 - 1))),
    "float": (pl.Float64(), st.one_of(st.none(), st.floats(allow_nan=True))),
    "bool": (pl.Boolean(), st.one_of(st.none(), st.booleans())),
    "date": (pl.Date(), st.one_of(st.none(), st.dates())),
    "utc": (pl.Datetime("us", "UTC"), st.one_of(st.none(), st.datetimes(timezones=st.just(UTC)))),
    "empty": (pl.Null(), st.none()),
}

HEADERS = [
    "name",
    "Name",
    "First Name",
    " padded ",
    "1st",
    "_private",
    "Gr" + chr(0xF6) + chr(0xDF) + "e",
    "a" * 70,
    "a" * 70 + "b",
    *RESERVED_COLUMNS,
]


@st.composite
def chunked_frames(draw: st.DrawFn) -> list[pl.DataFrame]:
    height = draw(st.integers(0, 8))
    headers = draw(st.lists(st.sampled_from(HEADERS), min_size=1, max_size=5, unique=True))
    series = []
    for header in headers:
        dtype, strategy = VALUES[draw(st.sampled_from(sorted(VALUES)))]
        values = draw(st.lists(strategy, min_size=height, max_size=height))
        series.append(pl.Series(header, values, dtype=dtype))
    frame = pl.DataFrame(series)
    cuts = sorted(draw(st.lists(st.integers(0, height), max_size=3)))
    return [frame[start:end] for start, end in pairwise([0, *cuts, height])]


def _tidy_model(value: str | None) -> str | None:
    if value is None:
        return None
    return value.strip(WHITE_SPACE) or None


@given(chunked_frames())
def test_common_transforms_twice_equal_once(chunks: list[pl.DataFrame]) -> None:
    once = list(transform(iter(chunks)))
    twice = list(transform(iter(once)))

    assert len(twice) == len(once)
    for first, second in zip(once, twice, strict=True):
        assert_frame_equal(second, first)


@given(chunked_frames())
def test_text_is_trimmed_and_empty_text_becomes_null(chunks: list[pl.DataFrame]) -> None:
    for before, after in zip(chunks, transform(iter(chunks)), strict=True):
        assert after.shape == before.shape
        assert after.dtypes == before.dtypes
        for old, new in zip(before.iter_columns(), after.iter_columns(), strict=True):
            if old.dtype == pl.String:
                assert new.to_list() == [_tidy_model(value) for value in old.to_list()]
            else:
                assert_frame_equal(new.to_frame(old.name), old.to_frame())


@given(chunked_frames())
def test_splitting_into_chunks_gives_the_same_result(chunks: list[pl.DataFrame]) -> None:
    whole = pl.concat(chunks)

    (expected,) = transform(iter([whole]))

    assert_frame_equal(pl.concat(list(transform(iter(chunks)))), expected)


def test_padded_text_becomes_trimmed_and_blank_text_null_but_near_misses_stay() -> None:
    frame = pl.DataFrame(
        {
            "Name": [
                "  Anna ",
                "   ",
                "",
                None,
                IDEOGRAPHIC_SPACE + "x" + EM_SPACE,
                FILE_SEPARATOR,
                ZERO_WIDTH_SPACE,
                "a  b",
            ]
        }
    )

    (result,) = transform(iter([frame]))

    assert result["name"].to_list() == [
        "Anna",
        None,
        None,
        None,
        "x",
        FILE_SEPARATOR,
        ZERO_WIDTH_SPACE,
        "a  b",
    ]


# --- custom transform.py: loading and its output contract ---------------------------------

CONTEXT = TransformContext(source="shop", dataset="orders", run_id=uuid7())
ROWS = pl.DataFrame({"id": [1, 2], "amount": [10, 20]})


def _transform_file(folder: Path, code: str) -> TransformFile:
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "transform.py").write_bytes(code.encode())
    file = find_transform(folder)
    assert file is not None
    return file


def _apply(file: TransformFile, chunks: list[pl.DataFrame]) -> list[pl.DataFrame]:
    return list(apply_transform(iter(chunks), load_transform(file), CONTEXT, file))


def test_a_source_without_transform_py_has_none(tmp_path: Path) -> None:
    assert find_transform(tmp_path) is None


def test_an_unreadable_transform_py_fails_naming_it(tmp_path: Path) -> None:
    (tmp_path / "transform.py").mkdir()

    with pytest.raises(TransformError, match="could not be read") as caught:
        find_transform(tmp_path)

    assert str(caught.value).startswith(f"{(tmp_path / 'transform.py').as_posix()}: ")


def test_the_hash_follows_the_bytes(tmp_path: Path) -> None:
    code = "def transform(df, context):\n    return df\n"
    first = _transform_file(tmp_path / "a", code)
    same = _transform_file(tmp_path / "b", code)
    edited = _transform_file(tmp_path / "c", code.replace("df\n", "df \n"))

    assert first.sha256 == same.sha256
    assert edited.sha256 != first.sha256
    assert first.code == code.encode()


def test_a_transform_gets_its_context_and_can_add_filter_and_drop(tmp_path: Path) -> None:
    file = _transform_file(
        tmp_path,
        "import polars as pl\n\n"
        "def transform(df, context):\n"
        "    return (\n"
        "        df.filter(pl.col('id') > 1)\n"
        "        .with_columns(dataset=pl.lit(context.dataset), total=pl.col('amount') * 2)\n"
        "        .drop('amount')\n"
        "    )\n",
    )

    (result,) = _apply(file, [ROWS])

    assert result.to_dicts() == [{"id": 2, "dataset": "orders", "total": 40}]
    assert not (tmp_path / "__pycache__").exists()


BROKEN = {
    "syntax error": (
        "def transform(df, context)\n    return df\n",
        "could not be loaded: SyntaxError",
    ),
    "error on load": ("raise RuntimeError('boom')\n", "could not be loaded: RuntimeError: boom"),
    "exit on load": ("raise SystemExit(3)\n", "could not be loaded: SystemExit: 3"),
    "no function": (
        "def transfrom(df, context):\n    return df\n",
        "defines no function 'transform'",
    ),
    "not callable": ("transform = 3\n", "defines no function 'transform'"),
    "raises": (
        "def transform(df, context):\n    raise ValueError('bad row')\n",
        "transform failed on dataset 'orders': ValueError: bad row",
    ),
    "returns None": ("def transform(df, context):\n    pass\n", "returned NoneType, expected"),
    "returns lazy": (
        "def transform(df, context):\n    return df.lazy()\n",
        "returned LazyFrame, expected a polars DataFrame",
    ),
    "capital name": (
        "def transform(df, context):\n    return df.rename({'amount': 'Total'})\n",
        "returned column 'Total', which is not a clean column name",
    ),
    "space in name": (
        "def transform(df, context):\n    return df.rename({'amount': 'a b'})\n",
        "use 'a_b'",
    ),
    "trailing underscore": (
        "def transform(df, context):\n    return df.rename({'amount': 'x_'})\n",
        "returned column 'x_', which is not a clean column name",
    ),
    "platform column": (
        "def transform(df, context):\n    return df.rename({'amount': '_run_id'})\n",
        "returned column '_run_id', which is a platform column",
    ),
    "list column": (
        "import polars as pl\n\ndef transform(df, context):\n"
        "    return df.with_columns(pl.concat_list('id', 'amount').alias('pair'))\n",
        "returned column 'pair', which has type List(Int64) that cannot be stored",
    ),
    "no columns": (
        "def transform(df, context):\n    return df.select()\n",
        "transform returned no columns",
    ),
    "schema changes": (
        "import polars as pl\n\ndef transform(df, context):\n"
        "    return df if df.height == 1 else df.with_columns(extra=pl.lit(1))\n",
        "differs from the first chunk's",
    ),
}


@pytest.mark.parametrize("case", sorted(BROKEN))
def test_a_broken_transform_fails_naming_its_file(tmp_path: Path, case: str) -> None:
    code, message = BROKEN[case]
    file = _transform_file(tmp_path / "shop", code)

    with pytest.raises(TransformError) as caught:
        _apply(file, [ROWS[:1], ROWS])

    assert str(caught.value).startswith(f"{(tmp_path / 'shop' / 'transform.py').as_posix()}: ")
    assert message in str(caught.value)
    assert not (tmp_path / "shop" / "__pycache__").exists()


def test_the_error_from_a_raising_transform_keeps_the_users_line(tmp_path: Path) -> None:
    file = _transform_file(tmp_path, BROKEN["raises"][0])

    with pytest.raises(TransformError) as caught:
        _apply(file, [ROWS])

    assert 'transform.py", line 2' in "".join(traceback.format_exception(caught.value))


# --- custom and common transforms in whole runs -------------------------------------------

TWO_DATASETS = (
    "connection:\n  type: csv\ndatasets:\n"
    "  - name: orders\n    path: orders.csv\n{settings}"
    "  - name: returns\n    path: returns.csv\n{settings}"
)


def _shop(tmp_path: Path, settings: str = "") -> Path:
    sources = tmp_path / "sources"
    folder = sources / "shop"
    folder.mkdir(parents=True)
    (folder / "source.yaml").write_text(TWO_DATASETS.format(settings=settings), encoding="utf-8")
    for name in ("orders", "returns"):
        (folder / f"{name}.csv").write_text(
            'id,customer,amount\n1,"  Anna ",10\n2,"   ",20\n3,"",30\n', encoding="utf-8"
        )
    return sources


def _run(sources: Path, loader: MemoryLoader) -> list[str]:
    config = load_source(sources, "shop", {})
    return [outcome.status for outcome in run_source("shop", config, sources, loader)]


def test_text_is_tidied_in_every_dataset(tmp_path: Path) -> None:
    sources = _shop(tmp_path)
    loader = MemoryLoader()

    assert _run(sources, loader) == ["succeeded", "succeeded"]

    for table in ("shop__orders", "shop__returns"):
        assert loader.tables[table]["customer"].to_list() == ["Anna", None, None]


def test_transform_py_applies_to_every_dataset_and_can_make_the_watermark(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging()
    merge = "    load_mode: merge\n    watermark: version\n    primary_key: [id]\n"
    sources = _shop(tmp_path, merge)
    (sources / "shop" / "transform.py").write_text(
        "import polars as pl\n\n"
        "def transform(df, context):\n"
        "    return df.with_columns(\n"
        "        source=pl.lit(context.source),\n"
        "        dataset=pl.lit(context.dataset),\n"
        "        version=pl.col('amount') // 10,\n"
        "    )\n",
        encoding="utf-8",
    )
    loader = MemoryLoader()

    assert _run(sources, loader) == ["succeeded", "succeeded"]

    for dataset in ("orders", "returns"):
        table = loader.tables[f"shop__{dataset}"]
        assert table.select("id", "source", "dataset", "version").rows() == [
            (1, "shop", dataset, 1),
            (2, "shop", dataset, 2),
            (3, "shop", dataset, 3),
        ]
        assert loader.states[("shop", dataset)].watermark == 3
    assert not (sources / "shop" / "__pycache__").exists()
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    applied = [e for e in events if e["event"] == "custom transform applied"]
    assert [(e["dataset"], e["rows_in"], e["rows_out"]) for e in applied] == [
        ("orders", 3, 3),
        ("returns", 3, 3),
    ]
    assert applied[0]["path"] == (sources / "shop" / "transform.py").as_posix()


def test_a_broken_transform_py_fails_every_dataset(tmp_path: Path) -> None:
    sources = _shop(tmp_path)
    (sources / "shop" / "transform.py").write_text(
        "def transform(df, context):\n    return df.rename({'amount': 'Amount'})\n",
        encoding="utf-8",
    )
    loader = MemoryLoader()

    assert _run(sources, loader) == ["failed", "failed"]

    for run in loader.runs.values():
        assert run["error_class"] == "TransformError"
        assert run["error_message"].startswith(
            f"{(sources / 'shop' / 'transform.py').as_posix()}: "
        )
    assert loader.tables == {}
