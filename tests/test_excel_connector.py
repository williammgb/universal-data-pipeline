import tempfile
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import pytest
import xlsxwriter  # type: ignore[import-untyped]
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from polars.testing import assert_frame_equal

from udp.config.source import load_source
from udp.connectors.base import ExtractRequest
from udp.connectors.excel import ExcelConnection, ExcelConnector, ExcelDataset
from udp.errors import ConfigError, ExtractError


def _extract(folder: Path, chunk_size: int, sheet: str | None = None) -> Iterator[pl.DataFrame]:
    request = ExtractRequest(
        source_dir=folder,
        connection=ExcelConnection(type="excel"),
        dataset=ExcelDataset(name="data", path="data.xlsx", sheet=sheet),
        chunk_size=chunk_size,
    )
    return ExcelConnector().extract(request)


COLUMN_KINDS: dict[str, tuple[pl.DataType, st.SearchStrategy[Any]]] = {
    "int": (pl.Int64(), st.integers(-(10**9), 10**9)),
    "float": (pl.Float64(), st.floats(-1e9, 1e9, allow_nan=False)),
    "text": (pl.String(), st.one_of(st.text(max_size=10), st.sampled_from(["007", "", "é漢字"]))),
    "bool": (pl.Boolean(), st.booleans()),
    "date": (pl.Date(), st.dates(min_value=date(1950, 1, 1), max_value=date(2100, 1, 1))),
}


@st.composite
def sheets(draw: st.DrawFn) -> pl.DataFrame:
    kinds = draw(st.lists(st.sampled_from(sorted(COLUMN_KINDS)), min_size=1, max_size=4))
    height = draw(st.integers(0, 300))
    columns = []
    for index, kind in enumerate(kinds):
        dtype, values = COLUMN_KINDS[kind]
        cells = draw(st.lists(st.none() | values, min_size=height, max_size=height))
        columns.append(pl.Series(f"col {index}", cells, dtype=dtype))
    return pl.DataFrame(columns)


# Each example writes and reads a spreadsheet, so a quarter of the profile's examples.
@settings(
    max_examples=settings().max_examples // 4,
    suppress_health_check=[HealthCheck.too_slow],
)
@given(sheets(), st.data())
def test_chunks_are_bounded_and_join_back_into_the_whole_sheet(
    sheet: pl.DataFrame, data: st.DataObject
) -> None:
    chunk_size = data.draw(st.integers(1, sheet.height + 2), label="chunk_size")
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "data.xlsx"
        sheet.write_excel(path, worksheet="rows")

        chunks = list(_extract(Path(directory), chunk_size))
        whole = pl.read_excel(path, sheet_id=1, engine="calamine", infer_schema_length=None)

    assert len(chunks) >= 1
    assert all(chunk.height <= chunk_size for chunk in chunks)
    assert len({tuple(chunk.schema.items()) for chunk in chunks}) == 1
    assert_frame_equal(pl.concat(chunks), whole)


def test_a_late_text_value_makes_the_column_text_without_losing_values(tmp_path: Path) -> None:
    workbook = xlsxwriter.Workbook(tmp_path / "data.xlsx")
    sheet = workbook.add_worksheet("rows")
    sheet.write_string(0, 0, "code")
    for row in range(1, 1101):
        sheet.write_number(row, 0, row)
    sheet.write_string(1101, 0, "not a number")
    workbook.close()

    (chunk,) = list(_extract(tmp_path, 5000))

    assert chunk.schema["code"] == pl.String
    assert chunk.height == 1101
    assert chunk["code"].null_count() == 0
    assert chunk["code"][-1] == "not a number"


def test_missing_file_names_the_file(tmp_path: Path) -> None:
    with pytest.raises(ExtractError, match=r"data\.xlsx"):
        list(_extract(tmp_path, 10))


def test_missing_sheet_names_the_file_and_sheet(tmp_path: Path) -> None:
    pl.DataFrame({"a": [1]}).write_excel(tmp_path / "data.xlsx", worksheet="rows")

    with pytest.raises(ExtractError, match=r"data\.xlsx.*'nope'"):
        list(_extract(tmp_path, 10, sheet="nope"))


def test_header_only_sheet_yields_one_empty_chunk_with_its_columns(tmp_path: Path) -> None:
    pl.DataFrame(schema={"a": pl.String, "b": pl.String}).write_excel(tmp_path / "data.xlsx")

    chunks = list(_extract(tmp_path, 10))

    assert len(chunks) == 1
    assert chunks[0].columns == ["a", "b"]
    assert chunks[0].height == 0


def test_path_outside_the_source_folder_is_rejected(tmp_path: Path) -> None:
    folder = tmp_path / "sources" / "sheets"
    folder.mkdir(parents=True)
    (folder / "source.yaml").write_text(
        "connection:\n  type: excel\ndatasets:\n  - name: data\n    path: ../x.xlsx\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match=r"datasets\[0\]\.path"):
        load_source(tmp_path / "sources", "sheets")
