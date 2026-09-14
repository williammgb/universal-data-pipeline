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
    LargeBinary,
    MetaData,
    Numeric,
    String,
    Table,
    create_engine,
    insert,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from udp.connectors.base import ExtractRequest
from udp.connectors.database import (
    DatabaseConnection,
    DatabaseConnector,
    DatabaseDataset,
    engine_url,
)
from udp.errors import ExtractError
from udp.log import configure_logging


def _sqlite_url(path: Path) -> str:
    return f"sqlite:///{path.as_posix()}"


def _extract(
    url: str, table: str, chunk_size: int, connector: DatabaseConnector | None = None
) -> Iterator[pl.DataFrame]:
    request = ExtractRequest(
        source_dir=Path("."),
        connection=DatabaseConnection(type="database", url=SecretStr(url)),
        dataset=DatabaseDataset(name="data", table=table),
        chunk_size=chunk_size,
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
        "amount": pl.String,
        "ratio": pl.Float64,
        "paid": pl.Boolean,
        "ordered_on": pl.Date,
        "updated_at": pl.Datetime("us"),
        "note": pl.String,
    }
    assert chunk.row(0) == (
        1,
        "12.30",
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

    with pytest.raises(ExtractError, match="photo"):
        list(_extract(_sqlite_url(path), "items", 10))


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
