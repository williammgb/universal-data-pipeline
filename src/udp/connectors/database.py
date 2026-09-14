import json
import time
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote

import polars as pl
from pydantic import ConfigDict, Field, SecretStr, field_validator
from sqlalchemy import URL, Column, Engine, MetaData, Table, create_engine, make_url, select, types
from sqlalchemy.exc import ArgumentError, NoSuchTableError, OperationalError, SQLAlchemyError

from udp.connectors.base import ConnectionBase, DatasetBase, ExtractRequest
from udp.connectors.retry import RETRY_WAITS, retry
from udp.errors import ExtractError

Convert = Callable[[Any], Any] | None


class DatabaseConnection(ConnectionBase):
    type: Literal["database"]
    url: SecretStr

    @field_validator("url")
    @classmethod
    def _is_sqlalchemy_url(cls, value: SecretStr) -> SecretStr:
        try:
            parsed = make_url(value.get_secret_value())
        except ArgumentError:
            raise ValueError("not a valid SQLAlchemy URL") from None
        if "@" in (parsed.host or ""):
            # An unencoded @ in the password moves part of it into the host name.
            raise ValueError(
                "not a valid SQLAlchemy URL: percent-encode special characters in the "
                "user name and password (@ as %40)"
            )
        return value


class DatabaseDataset(DatasetBase):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    table: str
    table_schema: str | None = Field(default=None, alias="schema")


def engine_url(url: str) -> URL:
    """The URL to connect with; plain postgresql:// uses the psycopg driver."""
    parsed = make_url(url)
    if parsed.drivername == "postgresql":
        parsed = parsed.set(drivername="postgresql+psycopg")
    return parsed


def _json_text(value: Any) -> str:
    return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)


def column_type(column: Column[Any]) -> tuple[pl.DataType, Convert]:
    """The Polars type a reflected column is read as, and how each value is converted."""
    kind = column.type
    if isinstance(kind, types.Boolean):
        return pl.Boolean(), None
    if isinstance(kind, types.Integer):
        return pl.Int64(), None
    if isinstance(kind, types.Float):
        return pl.Float64(), float
    if isinstance(kind, types.Numeric):
        return pl.String(), str
    if isinstance(kind, types.DateTime):
        return (pl.Datetime("us", "UTC") if kind.timezone else pl.Datetime("us")), None
    if isinstance(kind, types.Date):
        return pl.Date(), None
    if isinstance(kind, types.Time) and not kind.timezone:
        return pl.Time(), None
    if isinstance(kind, types.Uuid):
        return pl.String(), str
    if isinstance(kind, types.JSON):
        return pl.String(), _json_text
    if isinstance(kind, types.String):
        return pl.String(), None
    raise ExtractError(f"column '{column.name}' has type {kind}, which cannot be read yet")


def _frame(
    rows: Sequence[Sequence[Any]], columns: list[tuple[str, pl.DataType, Convert]], table: str
) -> pl.DataFrame:
    values_by_column = list(zip(*rows, strict=True)) if rows else [() for _ in columns]
    series = []
    for (name, dtype, convert), values in zip(columns, values_by_column, strict=True):
        if convert is not None:
            values = tuple(None if value is None else convert(value) for value in values)
        try:
            series.append(pl.Series(name, values, dtype=dtype, strict=True))
        except (TypeError, ValueError, OverflowError, pl.exceptions.PolarsError) as error:
            raise ExtractError(
                f"column '{name}' of table '{table}' holds a value that is not {dtype}: {error}"
            ) from error
    return pl.DataFrame(series)


class DatabaseConnector:
    connection_model = DatabaseConnection
    dataset_model = DatabaseDataset

    def __init__(
        self, waits: Sequence[float] = RETRY_WAITS, sleep: Callable[[float], None] = time.sleep
    ) -> None:
        self._waits = waits
        self._sleep = sleep

    def extract(
        self, request: ExtractRequest[DatabaseConnection, DatabaseDataset]
    ) -> Iterator[pl.DataFrame]:
        url = engine_url(request.connection.url.get_secret_value())
        shown = url.render_as_string(hide_password=True)
        table_name = request.dataset.table
        is_sqlite_file = url.get_backend_name() == "sqlite" and url.database not in (
            None,
            "",
            ":memory:",
        )
        if is_sqlite_file and not Path(str(url.database)).is_file():
            raise ExtractError(f"SQLite database file not found: {shown}")

        engine: Engine | None = None
        try:
            engine = create_engine(url)
            connection = retry(
                engine.connect,
                transient=lambda error: isinstance(error, OperationalError),
                waits=self._waits,
                sleep=self._sleep,
            )
            with connection:
                table = Table(
                    table_name,
                    MetaData(),
                    schema=request.dataset.table_schema,
                    autoload_with=connection,
                )
                columns = [(c.name, *column_type(c)) for c in table.columns]
                result = connection.execution_options(
                    stream_results=True, yield_per=request.chunk_size
                ).execute(select(table))
                yielded = False
                for rows in result.partitions():
                    yielded = True
                    yield _frame(rows, columns, table_name)
                if not yielded:
                    yield _frame([], columns, table_name)
        except NoSuchTableError:
            raise ExtractError(f"table '{table_name}' not found in {shown}") from None
        except SQLAlchemyError as error:
            # The driver's own exception is not chained: its text and traceback can
            # carry connection details, so only the password-free message is kept.
            message = f"could not read table '{table_name}' from {shown}: {error}"
            if url.password:
                for form in {str(url.password), quote(str(url.password), safe="")}:
                    message = message.replace(form, "***")
            raise ExtractError(message) from None
        finally:
            if engine is not None:
                engine.dispose()
