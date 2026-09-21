import json
import time
from collections.abc import Callable, Iterator, Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal, Self
from urllib.parse import quote

import polars as pl
import structlog
from pydantic import ConfigDict, Field, SecretStr, field_validator, model_validator
from sqlalchemy import URL, Column, Engine, MetaData, Table, create_engine, make_url, select, types
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import ArgumentError, NoSuchTableError, OperationalError, SQLAlchemyError

from udp.connectors.base import ConnectionBase, DatasetBase, ExtractRequest, FileVersion
from udp.connectors.retry import RETRY_WAITS, retry
from udp.errors import ExtractError
from udp.pipeline.transform import clean_column_names

log = structlog.get_logger(step="extract")

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
    # Source column names, as the table has them, that are never read.
    exclude_columns: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _keeps_the_columns_it_loads_by(self) -> Self:
        needed = {self.watermark, *(self.primary_key or [])} - {None}
        for name in self.exclude_columns:
            (clean,) = clean_column_names([name])
            if name in needed or clean in needed:
                raise ValueError(
                    f"exclude_columns: '{name}' is the watermark or part of the primary key"
                )
        return self


def engine_url(url: str) -> URL:
    """The URL to connect with; plain postgresql:// uses the psycopg driver."""
    parsed = make_url(url)
    if parsed.drivername == "postgresql":
        parsed = parsed.set(drivername="postgresql+psycopg")
    return parsed


def _json_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    # default=str: an array of dates or decimals is still written, as their text.
    return json.dumps(value, ensure_ascii=False, default=str)


def _finite(value: Decimal) -> Decimal:
    """Postgres numeric may hold NaN; an exact decimal cannot, and Polars panics on one."""
    if not value.is_finite():
        raise ValueError(f"{value} is not a finite number")
    return value


# The widest exact decimal Polars holds; a wider numeric is read as its text.
MAX_DECIMAL_PRECISION = 38


def _underlying(kind: types.TypeEngine[Any]) -> types.TypeEngine[Any]:
    """A domain's base type: a domain is a named built-in type with a rule attached."""
    while isinstance(kind, postgresql.DOMAIN):
        kind = kind.data_type
    return kind


def column_type(column: Column[Any]) -> tuple[pl.DataType, Convert]:
    """The Polars type a reflected column is read as, and how each value is converted.

    Binary columns are refused, because no text form of them is worth storing. Any other type
    this function does not know is read as its text, with a warning naming the column.
    """
    kind = _underlying(column.type)
    if isinstance(kind, types.Boolean):
        return pl.Boolean(), None
    if isinstance(kind, types.Integer):
        return pl.Int64(), None
    if isinstance(kind, types.Float):
        return pl.Float64(), float
    if isinstance(kind, types.Numeric):
        precision, scale = kind.precision, kind.scale
        if precision is not None and scale is not None and precision <= MAX_DECIMAL_PRECISION:
            return pl.Decimal(precision, scale), _finite
        # Unbounded numeric has no fixed number of decimals to hold it in, so it stays exact
        # as text until the dataset declares a type for it.
        return pl.String(), str
    if isinstance(kind, types.DateTime):
        return (pl.Datetime("us", "UTC") if kind.timezone else pl.Datetime("us")), None
    if isinstance(kind, types.Date):
        return pl.Date(), None
    if isinstance(kind, types.Time) and not kind.timezone:
        return pl.Time(), None
    if isinstance(kind, types.Uuid):
        return pl.String(), str
    if isinstance(kind, types.JSON | types.ARRAY):
        return pl.String(), _json_text
    if isinstance(kind, types.String):
        return pl.String(), None
    if isinstance(kind, types.LargeBinary | types.BINARY | types.VARBINARY):
        raise ExtractError(
            f"column '{column.name}' has type {kind}, which cannot be read; "
            "leave it out with exclude_columns"
        )
    log.warning("column read as text", column=column.name, type=str(column.type))
    return pl.String(), str


def _frame(
    rows: Sequence[Sequence[Any]], columns: list[tuple[str, pl.DataType, Convert]], table: str
) -> pl.DataFrame:
    values_by_column = list(zip(*rows, strict=True)) if rows else [() for _ in columns]
    series = []
    for (name, dtype, convert), values in zip(columns, values_by_column, strict=True):
        try:
            if convert is not None:
                values = tuple(None if value is None else convert(value) for value in values)
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

    def file_version(
        self, request: ExtractRequest[DatabaseConnection, DatabaseDataset]
    ) -> FileVersion | None:
        return None

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
                excluded = request.dataset.exclude_columns
                missing = [name for name in excluded if name not in table.columns]
                if missing:
                    raise ExtractError(
                        f"exclude_columns names {', '.join(repr(m) for m in missing)}, "
                        f"which table '{table_name}' does not have"
                    )
                kept = [c for c in table.columns if c.name not in excluded]
                columns = [(c.name, *column_type(c)) for c in kept]
                query = select(*kept)
                saved = request.watermark
                if saved is not None:
                    column = table.columns.get(saved.column)
                    if column is None:
                        log.info("watermark not pushed to source", column=saved.column)
                    elif saved.inclusive:
                        query = query.where(column >= saved.value)
                    else:
                        query = query.where(column > saved.value)
                result = connection.execution_options(
                    stream_results=True, yield_per=request.chunk_size
                ).execute(query)
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
