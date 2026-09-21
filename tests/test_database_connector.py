import tempfile
import traceback
from collections import Counter
from collections.abc import Iterator
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from pydantic import SecretStr, ValidationError
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    create_engine,
    insert,
    text,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from udp.connectors.base import ExtractRequest, SavedWatermark
from udp.connectors.database import (
    DatabaseConnection,
    DatabaseConnector,
    DatabaseDataset,
    _frame,
    column_type,
    engine_url,
)
from udp.errors import ExtractError
from udp.log import configure_logging


def _sqlite_url(path: Path) -> str:
    return f"sqlite:///{path.as_posix()}"


def _extract(
    url: str,
    table: str,
    chunk_size: int,
    connector: DatabaseConnector | None = None,
    watermark: SavedWatermark | None = None,
    exclude: list[str] | None = None,
) -> Iterator[pl.DataFrame]:
    request = ExtractRequest(
        source_dir=Path("."),
        connection=DatabaseConnection(type="database", url=SecretStr(url)),
        dataset=DatabaseDataset(name="data", table=table, exclude_columns=exclude or []),
        chunk_size=chunk_size,
        watermark=watermark,
    )
    return (connector or DatabaseConnector(waits=())).extract(request)


def _create(path: Path, columns: list[Column[Any]], rows: list[dict[str, Any]]) -> None:
    engine = create_engine(_sqlite_url(path))
    table = Table("items", MetaData(), *columns)
    with engine.begin() as connection:
        table.create(connection)
        if rows:
            connection.execute(insert(table), rows)
    engine.dispose()


COLUMN_KINDS: dict[str, tuple[Any, st.SearchStrategy[Any]]] = {
    "integer": (BigInteger, st.integers(-(2**63), 2**63 - 1)),
    "real": (Float, st.floats(allow_nan=False)),
    "text": (String, st.text(max_size=10)),
    "boolean": (Boolean, st.booleans()),
    "date": (Date, st.dates()),
    "datetime": (DateTime, st.datetimes()),
    "numeric": (Numeric(12, 2), st.decimals(-(10**9), 10**9, places=2)),
}


@st.composite
def tables(draw: st.DrawFn) -> tuple[list[Column[Any]], list[dict[str, Any]]]:
    kinds = draw(st.lists(st.sampled_from(sorted(COLUMN_KINDS)), min_size=1, max_size=5))
    height = draw(st.integers(0, 60))
    empty_prefix = draw(st.integers(0, height))
    columns = [Column(f"c{i}_{kind}", COLUMN_KINDS[kind][0]) for i, kind in enumerate(kinds)]
    rows = []
    for row in range(height):
        values = {}
        for index, kind in enumerate(kinds):
            leading_nulls = index == 0 and row < empty_prefix
            values[f"c{index}_{kind}"] = (
                None if leading_nulls else draw(st.none() | COLUMN_KINDS[kind][1])
            )
        rows.append(values)
    return columns, rows


# Each example builds a database file, so a quarter of the profile's examples.
@settings(
    max_examples=settings().max_examples // 4,
    suppress_health_check=[HealthCheck.too_slow],
)
@given(tables(), st.data())
def test_chunks_are_bounded_typed_from_the_table_and_complete(
    table: tuple[list[Column[Any]], list[dict[str, Any]]], data: st.DataObject
) -> None:
    columns, rows = table
    chunk_size = data.draw(st.integers(1, len(rows) + 2), label="chunk_size")
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "source.db"
        _create(path, columns, rows)
        url = _sqlite_url(path)

        chunks = list(_extract(url, "items", chunk_size))
        (whole,) = list(_extract(url, "items", len(rows) + 1))

    schema = whole.schema
    assert all(chunk.height <= chunk_size for chunk in chunks)
    assert all(chunk.schema == schema for chunk in chunks)
    assert sum(chunk.height for chunk in chunks) == len(rows)
    assert Counter(pl.concat(chunks).rows()) == Counter(whole.rows())


def test_column_types_are_taken_from_the_table_definition(tmp_path: Path) -> None:
    path = tmp_path / "source.db"
    _create(
        path,
        [
            Column("id", BigInteger),
            Column("amount", Numeric(10, 2)),
            Column("ratio", Float),
            Column("paid", Boolean),
            Column("ordered_on", Date),
            Column("updated_at", DateTime),
            Column("note", String),
        ],
        [
            {
                "id": 1,
                "amount": Decimal("12.30"),
                "ratio": 0.5,
                "paid": True,
                "ordered_on": date(2024, 1, 2),
                "updated_at": datetime(2024, 1, 2, 3, 4, 5),
                "note": None,
            }
        ],
    )

    (chunk,) = list(_extract(_sqlite_url(path), "items", 10))

    assert dict(chunk.schema) == {
        "id": pl.Int64,
        "amount": pl.Decimal(10, 2),
        "ratio": pl.Float64,
        "paid": pl.Boolean,
        "ordered_on": pl.Date,
        "updated_at": pl.Datetime("us"),
        "note": pl.String,
    }
    assert chunk.row(0) == (
        1,
        Decimal("12.30"),
        0.5,
        True,
        date(2024, 1, 2),
        datetime(2024, 1, 2, 3, 4, 5),
        None,
    )


def test_empty_table_yields_one_empty_chunk_with_the_schema(tmp_path: Path) -> None:
    path = tmp_path / "source.db"
    _create(path, [Column("id", BigInteger), Column("name", String)], [])

    chunks = list(_extract(_sqlite_url(path), "items", 10))

    assert len(chunks) == 1
    assert chunks[0].columns == ["id", "name"]
    assert chunks[0].height == 0


def test_unsupported_column_type_names_the_column(tmp_path: Path) -> None:
    path = tmp_path / "source.db"
    _create(path, [Column("id", BigInteger), Column("photo", LargeBinary)], [])

    with pytest.raises(ExtractError, match=r"photo.*exclude_columns"):
        list(_extract(_sqlite_url(path), "items", 10))


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        # A domain is read as the type underneath it, however deeply nested.
        (postgresql.DOMAIN("year", Integer()), pl.Int64()),
        (postgresql.DOMAIN("price", postgresql.DOMAIN("money2", Numeric(8, 2))), pl.Decimal(8, 2)),
        (postgresql.ENUM("G", "PG", name="rating"), pl.String()),
        (postgresql.ARRAY(Text()), pl.String()),
        (postgresql.TSVECTOR(), pl.String()),
        (postgresql.INTERVAL(), pl.String()),
        (Numeric(38, 10), pl.Decimal(38, 10)),
        (Numeric(39, 2), pl.String()),
        (Numeric(), pl.String()),
    ],
)
def test_each_column_type_is_read_as(kind: Any, expected: pl.DataType) -> None:
    dtype, _ = column_type(Column("c", kind))

    assert dtype == expected


def test_a_type_read_as_text_says_so_once_naming_the_column(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging()
    _, convert = column_type(Column("fulltext", postgresql.TSVECTOR()))

    assert convert is not None and convert("'dvd':1") == "'dvd':1"
    line = capsys.readouterr().out.strip()
    assert "column read as text" in line and "fulltext" in line and "TSVECTOR" in line


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_a_numeric_that_is_not_a_number_fails_naming_the_column(value: str) -> None:
    rows = [(Decimal("1.50"),), (Decimal(value),)]
    columns = [("rate", *column_type(Column("rate", Numeric(10, 2))))]

    with pytest.raises(ExtractError, match=r"column 'rate' of table 'items'.*not a finite"):
        _frame(rows, columns, "items")


def test_an_array_is_read_as_json_text() -> None:
    _, convert = column_type(Column("tags", postgresql.ARRAY(Text())))

    assert convert is not None
    assert convert(["Trailers", "Deleted Scenes"]) == '["Trailers", "Deleted Scenes"]'
    assert convert([date(2024, 1, 2), Decimal("1.50")]) == '["2024-01-02", "1.50"]'


def test_excluded_columns_are_never_read(tmp_path: Path) -> None:
    path = tmp_path / "source.db"
    _create(
        path,
        [Column("id", BigInteger), Column("photo", LargeBinary), Column("name", String)],
        [{"id": 1, "photo": b"\x00\x01", "name": "a"}],
    )

    (chunk,) = list(_extract(_sqlite_url(path), "items", 10, exclude=["photo"]))

    assert chunk.columns == ["id", "name"]
    assert chunk.row(0) == (1, "a")


def test_excluding_a_column_the_table_lacks_names_it(tmp_path: Path) -> None:
    path = tmp_path / "source.db"
    _create(path, [Column("id", BigInteger)], [])

    with pytest.raises(ExtractError, match=r"'gone'.*does not have"):
        list(_extract(_sqlite_url(path), "items", 10, exclude=["gone"]))


@pytest.mark.parametrize(
    "fields",
    [
        {"load_mode": "append", "watermark": "changed", "exclude_columns": ["changed"]},
        {
            "load_mode": "merge",
            "watermark": "changed",
            "primary_key": ["id"],
            "exclude_columns": ["id"],
        },
    ],
)
def test_the_watermark_and_primary_key_cannot_be_excluded(fields: dict[str, Any]) -> None:
    with pytest.raises(ValidationError, match="watermark or part of the primary key"):
        DatabaseDataset(name="items", table="items", **fields)


def test_text_stored_in_an_integer_column_names_the_column(tmp_path: Path) -> None:
    path = tmp_path / "source.db"
    _create(path, [Column("id", BigInteger)], [{"id": 1}])
    engine = create_engine(_sqlite_url(path))
    with engine.begin() as connection:
        connection.execute(text("INSERT INTO items (id) VALUES ('not a number')"))
    engine.dispose()

    with pytest.raises(ExtractError, match="'id'"):
        list(_extract(_sqlite_url(path), "items", 10))


def test_missing_table_raises(tmp_path: Path) -> None:
    path = tmp_path / "source.db"
    _create(path, [Column("id", BigInteger)], [])

    with pytest.raises(ExtractError, match="'orders' not found"):
        list(_extract(_sqlite_url(path), "orders", 10))


def test_missing_sqlite_file_raises_without_creating_it(tmp_path: Path) -> None:
    path = tmp_path / "absent.db"

    with pytest.raises(ExtractError, match="not found"):
        list(_extract(_sqlite_url(path), "items", 10))
    assert not path.exists()


WATERMARK_COLUMNS: dict[str, tuple[Any, st.SearchStrategy[Any]]] = {
    "integer": (BigInteger, st.integers(-(2**62), 2**62)),
    "date": (Date, st.dates()),
    "datetime": (DateTime, st.datetimes()),
}


@st.composite
def watermarked_tables(draw: st.DrawFn) -> tuple[Any, list[Any], Any, bool]:
    kind = draw(st.sampled_from(sorted(WATERMARK_COLUMNS)))
    column_type, values = WATERMARK_COLUMNS[kind]
    stored = draw(st.lists(st.none() | values, max_size=30))
    present = [value for value in stored if value is not None]
    saved = draw(st.sampled_from(present) | values if present else values)
    return column_type, stored, saved, draw(st.booleans())


@settings(
    max_examples=settings().max_examples // 4,
    suppress_health_check=[HealthCheck.too_slow],
)
@given(watermarked_tables())
def test_watermark_in_the_query_reads_the_same_rows_as_filtering_afterwards(
    generated: tuple[Any, list[Any], Any, bool],
) -> None:
    column_type, stored, saved, inclusive = generated
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "source.db"
        rows = [{"id": index, "changed": value} for index, value in enumerate(stored)]
        _create(path, [Column("id", BigInteger), Column("changed", column_type)], rows)
        url = _sqlite_url(path)

        pushed = pl.concat(
            _extract(url, "items", 7, watermark=SavedWatermark("changed", saved, inclusive))
        )
        everything = pl.concat(_extract(url, "items", 7))

    def newer(value: Any) -> bool:
        return value is not None and (value >= saved if inclusive else value > saved)

    expected = sorted(row["id"] for row in everything.to_dicts() if newer(row["changed"]))
    assert sorted(pushed["id"].to_list()) == expected


def test_watermark_on_an_unknown_column_reads_everything(tmp_path: Path) -> None:
    path = tmp_path / "source.db"
    _create(path, [Column("id", BigInteger)], [{"id": 1}, {"id": 2}])

    chunks = _extract(_sqlite_url(path), "items", 10, watermark=SavedWatermark("gone", 5, False))

    assert pl.concat(chunks).height == 2


def test_database_sources_have_no_file_version() -> None:
    request = ExtractRequest(
        source_dir=Path("."),
        connection=DatabaseConnection(type="database", url=SecretStr("sqlite://")),
        dataset=DatabaseDataset(name="data", table="items"),
        chunk_size=10,
    )

    assert DatabaseConnector().file_version(request) is None


def test_plain_postgresql_urls_use_the_psycopg_driver() -> None:
    assert engine_url("postgresql://u:p@host/db").drivername == "postgresql+psycopg"
    assert engine_url("mysql+pymysql://u:p@host/db").drivername == "mysql+pymysql"


def test_connection_errors_are_retried(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "source.db"
    _create(path, [Column("id", BigInteger)], [{"id": 1}, {"id": 2}])
    real_connect = Engine.connect
    calls = []

    def flaky(self: Engine) -> Any:
        calls.append(1)
        if len(calls) <= 2:
            raise OperationalError("connect", {}, Exception("server starting up"))
        return real_connect(self)

    monkeypatch.setattr(Engine, "connect", flaky)
    connector = DatabaseConnector(waits=(0, 0, 0), sleep=lambda _: None)

    chunks = list(_extract(_sqlite_url(path), "items", 10, connector))

    assert sum(chunk.height for chunk in chunks) == 2
    assert len(calls) == 3


@pytest.mark.parametrize("password", ["Leak-S3cret", "Leak%40S3cret"])
def test_connection_failure_never_shows_the_password(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], password: str
) -> None:
    url = f"postgresql://reader:{password}@127.0.0.1:1/source"

    def refuse(self: Engine) -> Any:
        # A driver that echoes its connection string is the worst case to guard against.
        raise OperationalError("connect", {}, Exception(f"could not connect to {url}"))

    monkeypatch.setattr(Engine, "connect", refuse)
    configure_logging()

    with pytest.raises(ExtractError) as raised:
        list(_extract(url, "orders", 10, DatabaseConnector(waits=(0,), sleep=lambda _: None)))

    shown = str(raised.value) + "".join(traceback.format_exception(raised.value))
    logged = capsys.readouterr().out
    assert '"event": "retrying"' in logged
    for secret in ("Leak-S3cret", "Leak@S3cret", "Leak%40S3cret"):
        assert secret not in shown
        assert secret not in logged
    assert "127.0.0.1" in shown


def test_unencoded_at_sign_in_the_password_is_rejected() -> None:
    with pytest.raises(ValidationError, match="percent-encode") as raised:
        DatabaseConnection(type="database", url=SecretStr("postgresql://reader:p@ss@db/source"))

    assert "p@ss" not in str(raised.value)


def test_missing_database_driver_is_an_extract_error() -> None:
    with pytest.raises(ExtractError, match="could not read table"):
        list(_extract("nosuchdialect://user:pw@host/db", "orders", 10))
