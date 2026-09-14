from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import datetime
from itertools import chain
from typing import Self
from uuid import UUID

import polars as pl
import psycopg
from psycopg import sql

from udp.errors import LoadError
from udp.storage.loader import RunFailure, RunStart, columns_mismatch, table_columns

_STAGE = sql.Identifier("udp_stage")


class PostgresTransaction:
    def __init__(self, conn: psycopg.Connection) -> None:
        self._conn = conn

    def replace_table(self, table: str, chunks: Iterable[pl.DataFrame]) -> int:
        remaining = iter(chunks)
        first = next(remaining, None)
        if first is None:
            raise LoadError(f"no data to load into datasets.{table}")
        columns = table_columns(first)
        names = [name for name, _ in columns]
        target = sql.Identifier("datasets", table)

        existing = self._conn.execute(
            "SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute "
            "WHERE attrelid = to_regclass(%s) AND attnum > 0 AND NOT attisdropped "
            "ORDER BY attnum",
            [f"datasets.{table}"],
        ).fetchall()
        if not existing:
            definition = sql.SQL(", ").join(
                sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(kind))
                for name, kind in columns
            )
            self._conn.execute(sql.SQL("CREATE TABLE {} ({})").format(target, definition))
        elif list(existing) != columns:
            raise columns_mismatch(table, list(existing), columns)

        self._conn.execute(sql.SQL("DROP TABLE IF EXISTS pg_temp.{}").format(_STAGE))
        self._conn.execute(
            sql.SQL("CREATE TEMP TABLE {} (LIKE {}) ON COMMIT DROP").format(_STAGE, target)
        )
        column_list = sql.SQL(", ").join(sql.Identifier(name) for name in names)
        copy_statement = sql.SQL("COPY {} ({}) FROM STDIN (FORMAT csv)").format(_STAGE, column_list)
        with self._conn.cursor() as cursor, cursor.copy(copy_statement) as copy:
            for chunk in chain([first], remaining):
                if chunk.columns != names:
                    raise LoadError(f"chunk columns {chunk.columns} differ from {names}")
                copy.write(chunk.write_csv(include_header=False, quote_style="non_numeric"))

        self._conn.execute(sql.SQL("TRUNCATE {}").format(target))
        inserted = self._conn.execute(
            sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                target, column_list, column_list, _STAGE
            )
        )
        return inserted.rowcount

    def succeed_run(
        self, run_id: UUID, *, ended_at: datetime, rows_extracted: int, rows_loaded: int
    ) -> None:
        updated = self._conn.execute(
            "UPDATE platform.pipeline_runs SET status = 'succeeded', ended_at = %s, "
            "rows_extracted = %s, rows_loaded = %s WHERE run_id = %s AND status = 'running'",
            [ended_at, rows_extracted, rows_loaded, run_id],
        )
        if updated.rowcount != 1:
            raise LoadError(f"run {run_id} is not a running run")


class PostgresLoader:
    """Writes to the platform database. Connects on first use."""

    def __init__(self, database_url: str) -> None:
        self._database_url = database_url
        self._conn: psycopg.Connection | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _connection(self) -> psycopg.Connection:
        if self._conn is None:
            self._conn = psycopg.connect(self._database_url, autocommit=True)
        return self._conn

    def start_run(self, run: RunStart) -> None:
        self._connection().execute(
            "INSERT INTO platform.pipeline_runs "
            "(run_id, source, dataset, trigger, status, started_at) "
            "VALUES (%s, %s, %s, %s, 'running', %s)",
            [run.run_id, run.source, run.dataset, run.trigger, run.started_at],
        )

    @contextmanager
    def transaction(self) -> Iterator[PostgresTransaction]:
        conn = self._connection()
        with conn.transaction():
            yield PostgresTransaction(conn)

    def fail_run(
        self,
        run_id: UUID,
        *,
        ended_at: datetime,
        rows_extracted: int | None,
        failure: RunFailure,
    ) -> None:
        updated = self._connection().execute(
            "UPDATE platform.pipeline_runs SET status = 'failed', ended_at = %s, "
            "rows_extracted = %s, error_class = %s, error_message = %s, error_traceback = %s "
            "WHERE run_id = %s AND status = 'running'",
            [
                ended_at,
                rows_extracted,
                failure.error_class,
                failure.message,
                failure.traceback,
                run_id,
            ],
        )
        if updated.rowcount != 1:
            raise LoadError(f"run {run_id} is not a running run")
