import csv
import tempfile
from collections.abc import Iterator
from pathlib import Path

import polars as pl
import pytest
from hypothesis import given
from hypothesis import strategies as st
from polars.testing import assert_frame_equal

from udp.connectors.base import ExtractRequest
from udp.connectors.csv import INFER_SCHEMA_ROWS, CsvConnection, CsvConnector, CsvDataset
from udp.errors import ExtractError

cell = st.one_of(
    st.just(""),
    st.sampled_from(["a,b", 'say "hi"', "line\nbreak", "win\r\nline", "007", "1.5", "-3", "true"]),
    st.integers(-(10**6), 10**6).map(str),
    st.text(alphabet=st.characters(blacklist_categories=["Cs", "Cc"]), max_size=12),
)


@st.composite
def csv_tables(draw: st.DrawFn) -> list[list[str]]:
    width = draw(st.integers(1, 5))
    header = [f"col{i}" for i in range(width)]
    rows = draw(st.lists(st.lists(cell, min_size=width, max_size=width), max_size=300))
    return [header, *rows]


def _extract(source_dir: Path, chunk_size: int) -> Iterator[pl.DataFrame]:
    request = ExtractRequest(
        source_dir=source_dir,
        connection=CsvConnection(type="csv"),
        dataset=CsvDataset(name="data", path="data.csv"),
        chunk_size=chunk_size,
    )
    return CsvConnector().extract(request)


@given(csv_tables(), st.data())
def test_chunks_are_bounded_and_join_back_into_the_whole_file(
    table: list[list[str]], data: st.DataObject
) -> None:
    rows = len(table) - 1
    chunk_size = data.draw(st.integers(1, rows + 2), label="chunk_size")
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "data.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            csv.writer(handle).writerows(table)

        chunks = list(_extract(Path(directory), chunk_size))
        whole = pl.read_csv(path, infer_schema_length=INFER_SCHEMA_ROWS)

    assert len(chunks) >= 1
    assert all(chunk.height <= chunk_size for chunk in chunks)
    assert_frame_equal(pl.concat(chunks), whole)


def test_missing_file_raises_extract_error_naming_the_path(tmp_path: Path) -> None:
    with pytest.raises(ExtractError, match=r"data\.csv"):
        list(_extract(tmp_path, 10))


def _request(source_dir: Path) -> ExtractRequest[CsvConnection, CsvDataset]:
    return ExtractRequest(
        source_dir=source_dir,
        connection=CsvConnection(type="csv"),
        dataset=CsvDataset(name="data", path="data.csv"),
        chunk_size=10,
    )


def test_file_version_follows_the_file_content(tmp_path: Path) -> None:
    path = tmp_path / "data.csv"
    path.write_bytes(b"a,b\n1,2\n")
    first = CsvConnector().file_version(_request(tmp_path))

    path.write_bytes(b"a,b\n1,2\n")
    same = CsvConnector().file_version(_request(tmp_path))
    path.write_bytes(b"a,b\n1,3\n")
    changed = CsvConnector().file_version(_request(tmp_path))

    assert first.path == "data.csv"
    assert first == same
    assert first.sha256 != changed.sha256


def test_file_version_of_a_missing_file_names_the_path(tmp_path: Path) -> None:
    with pytest.raises(ExtractError, match=r"data\.csv"):
        CsvConnector().file_version(_request(tmp_path))


def test_a_late_non_number_fails_undeclared_but_reads_as_text_when_declared(
    tmp_path: Path,
) -> None:
    rows = [str(i) for i in range(INFER_SCHEMA_ROWS)] + ["N/A"]
    (tmp_path / "data.csv").write_text(
        "Customer ID,amount\n" + "".join(f"{row},{i}\n" for i, row in enumerate(rows)),
        encoding="utf-8",
    )

    with pytest.raises(ExtractError, match=r"data\.csv"):
        list(_extract(tmp_path, 100_000))

    request = ExtractRequest(
        source_dir=tmp_path,
        connection=CsvConnection(type="csv"),
        dataset=CsvDataset(name="data", path="data.csv", columns={"customer_id": "integer"}),
        chunk_size=100_000,
    )
    (chunk,) = CsvConnector().extract(request)
    assert chunk.schema == pl.Schema({"Customer ID": pl.String, "amount": pl.Int64})
    assert chunk["Customer ID"][-1] == "N/A"


def test_header_only_file_yields_one_empty_chunk_with_the_schema(tmp_path: Path) -> None:
    (tmp_path / "data.csv").write_text("a,b\n", encoding="utf-8")

    chunks = list(_extract(tmp_path, 10))

    assert len(chunks) == 1
    assert chunks[0].columns == ["a", "b"]
    assert chunks[0].height == 0
