from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from datetime import date, datetime
from itertools import chain
from typing import Any, Self
from uuid import UUID

import polars as pl
import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from udp.errors import LoadError
from udp.storage.loader import (
    Column,
    ColumnChanges,
    DatasetState,
    LoadResult,
    RunFailure,
    RunFindings,
    RunStart,
    column_changes,
    table_columns,
    watermark_from_text,
    watermark_to_text,
)

_STAGE = sql.Identifier("udp_stage")
_STAGE_ROW = sql.Identifier("_stage_row")


def _identifiers(names: Sequence[str]) -> sql.Composed:
    return sql.SQL(", ").join(sql.Identifier(name) for name in names)


class PostgresTransaction:
    def __init__(self, conn: psycopg.Connection) -> None:
        self._conn = conn

    def _columns(self, table: str) -> list[Column]:
        rows = self._conn.execute(
            "SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute "
            "WHERE attrelid = to_regclass(%s) AND attnum > 0 AND NOT attisdropped "
            "ORDER BY attnum",
            [f"datasets.{table}"],
        ).fetchall()
        return [(name, kind) for name, kind in rows]

    def _stage(
        self,
        table: str,
        chunks: Iterable[pl.DataFrame],
        primary_key: Sequence[str] = (),
        numbered: bool = False,
    ) -> tuple[list[str], ColumnChanges]:
        """Create or widen the table, then COPY every chunk into a temporary stage table."""
        remaining = iter(chunks)
        first = next(remaining, None)
        if first is None:
            raise LoadError(f"no data to load into datasets.{table}")
        names = first.columns
        target = sql.Identifier("datasets", table)

        existing = self._columns(table)
        if existing:
            changes = column_changes(table, existing, first.schema)
            for name, kind in changes.added:
                self._conn.execute(
                    sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                        target, sql.Identifier(name), sql.SQL(kind)
                    )
                )
        else:
            changes = ColumnChanges((), ())
            definition = [
                sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(kind))
                for name, kind in table_columns(first)
            ]
            if primary_key:
                definition.append(sql.SQL("PRIMARY KEY ({})").format(_identifiers(primary_key)))
            self._conn.execute(
                sql.SQL("CREATE TABLE {} ({})").format(target, sql.SQL(", ").join(definition))
            )

        self._conn.execute(sql.SQL("DROP TABLE IF EXISTS pg_temp.{}").format(_STAGE))
        self._conn.execute(
            sql.SQL("CREATE TEMP TABLE {} (LIKE {}) ON COMMIT DROP").format(_STAGE, target)
        )
        if numbered:
            self._conn.execute(
                sql.SQL("ALTER TABLE {} ADD COLUMN {} bigint GENERATED ALWAYS AS IDENTITY").format(
                    _STAGE, _STAGE_ROW
                )
            )
        copy_statement = sql.SQL("COPY {} ({}) FROM STDIN (FORMAT csv)").format(
            _STAGE, _identifiers(names)
        )
        with self._conn.cursor() as cursor, cursor.copy(copy_statement) as copy:
            for chunk in chain([first], remaining):
                if chunk.columns != names:
                    raise LoadError(f"chunk columns {chunk.columns} differ from {names}")
                copy.write(chunk.write_csv(include_header=False, quote_style="non_numeric"))
        return names, changes

    def replace_table(self, table: str, chunks: Iterable[pl.DataFrame]) -> LoadResult:
        names, changes = self._stage(table, chunks)
        target = sql.Identifier("datasets", table)
        self._conn.execute(sql.SQL("TRUNCATE {}").format(target))
        inserted = self._conn.execute(
            sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                target, _identifiers(names), _identifiers(names), _STAGE
            )
        )
        return LoadResult(inserted.rowcount, changes.added, changes.missing)

    def append_rows(self, table: str, chunks: Iterable[pl.DataFrame]) -> LoadResult:
        names, changes = self._stage(table, chunks)
        inserted = self._conn.execute(
            sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                sql.Identifier("datasets", table), _identifiers(names), _identifiers(names), _STAGE
            )
        )
        return LoadResult(inserted.rowcount, changes.added, changes.missing)

    def merge_rows(
        self,
        table: str,
        chunks: Iterable[pl.DataFrame],
        *,
        primary_key: Sequence[str],
        watermark: str,
    ) -> LoadResult:
        names, changes = self._stage(table, chunks, primary_key, numbered=True)
        updates = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(name), sql.Identifier(name))
            for name in names
            if name not in primary_key
        )
        merged = self._conn.execute(
            sql.SQL(
                "INSERT INTO {target} AS stored ({columns}) "
                "SELECT DISTINCT ON ({key}) {columns} FROM {stage} "
                "ORDER BY {key}, {watermark} DESC NULLS LAST, {stage_row} DESC "
                "ON CONFLICT ({key}) DO UPDATE SET {updates} "
                "WHERE stored._record_hash IS DISTINCT FROM EXCLUDED._record_hash"
            ).format(
                target=sql.Identifier("datasets", table),
                columns=_identifiers(names),
                key=_identifiers(primary_key),
                stage=_STAGE,
                watermark=sql.Identifier(watermark),
                stage_row=_STAGE_ROW,
                updates=updates,
            )
        )
        return LoadResult(merged.rowcount, changes.added, changes.missing)

    def drop_table(self, table: str) -> None:
        self._conn.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier("datasets", table))
        )

    def read_state(self, source: str, dataset: str) -> DatasetState | None:
        row = self._conn.execute(
            "SELECT load_mode, primary_key, watermark_column, watermark_type, watermark, "
            "file_path, file_sha256, config_sha256, run_id, saved_at "
            "FROM platform.source_state WHERE source = %s AND dataset = %s",
            [source, dataset],
        ).fetchone()
        if row is None:
            return None
        mode, key, column, kind, mark, path, digest, config, run_id, saved_at = row
        return DatasetState(
            source=source,
            dataset=dataset,
            load_mode=mode,
            primary_key=tuple(key),
            watermark_column=column,
            watermark_type=kind,
            watermark=None if mark is None or kind is None else watermark_from_text(mark, kind),
            file_path=path,
            file_sha256=digest,
            config_sha256=config,
            run_id=run_id,
            saved_at=saved_at,
        )

    def save_state(self, state: DatasetState) -> None:
        values: list[Any] = [
            state.source,
            state.dataset,
            state.load_mode,
            list(state.primary_key),
            state.watermark_column,
            state.watermark_type,
            None if state.watermark is None else watermark_to_text(state.watermark),
            state.file_path,
            state.file_sha256,
            state.config_sha256,
            state.run_id,
            state.saved_at,
        ]
        self._conn.execute(
            "INSERT INTO platform.source_state (source, dataset, load_mode, primary_key, "
            "watermark_column, watermark_type, watermark, file_path, file_sha256, "
            "config_sha256, run_id, saved_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (source, dataset) DO UPDATE SET load_mode = EXCLUDED.load_mode, "
            "primary_key = EXCLUDED.primary_key, watermark_column = EXCLUDED.watermark_column, "
            "watermark_type = EXCLUDED.watermark_type, watermark = EXCLUDED.watermark, "
            "file_path = EXCLUDED.file_path, file_sha256 = EXCLUDED.file_sha256, "
            "config_sha256 = EXCLUDED.config_sha256, run_id = EXCLUDED.run_id, "
            "saved_at = EXCLUDED.saved_at",
            values,
        )

    def record_columns(
        self, table: str, source: str, dataset: str, run_id: UUID, recorded_at: datetime
    ) -> int | None:
        columns = [[name, kind] for name, kind in self._columns(table)]
        latest = self._conn.execute(
            "SELECT version, columns FROM platform.schema_versions "
            "WHERE source = %s AND dataset = %s ORDER BY version DESC LIMIT 1",
            [source, dataset],
        ).fetchone()
        if latest is not None and latest[1] == columns:
            return None
        version = 1 if latest is None else latest[0] + 1
        self._conn.execute(
            "INSERT INTO platform.schema_versions "
            "(source, dataset, version, columns, run_id, recorded_at) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            [source, dataset, version, Jsonb(columns), run_id, recorded_at],
        )
        return int(version)

    def table_rows(self, table: str) -> int:
        row = self._conn.execute(
            sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier("datasets", table))
        ).fetchone()
        return int(row[0]) if row else 0

    def duplicate_rows(self, table: str, columns: Sequence[str]) -> int:
        not_null = sql.SQL(" AND ").join(
            sql.SQL("{} IS NOT NULL").format(sql.Identifier(name)) for name in columns
        )
        row = self._conn.execute(
            sql.SQL(
                "SELECT coalesce(sum(n), 0) FROM (SELECT count(*) AS n FROM {} WHERE {} "
                "GROUP BY {} HAVING count(*) > 1) AS groups"
            ).format(sql.Identifier("datasets", table), not_null, _identifiers(columns))
        ).fetchone()
        return int(row[0]) if row else 0

    def newest_value(self, table: str, column: str) -> date | datetime | None:
        row = self._conn.execute(
            sql.SQL("SELECT max({}) FROM {}").format(
                sql.Identifier(column), sql.Identifier("datasets", table)
            )
        ).fetchone()
        value = row[0] if row else None
        return value if isinstance(value, date) else None

    def previous_table_rows(self, source: str, dataset: str) -> int | None:
        row = self._conn.execute(
            "SELECT results.table_rows FROM platform.quality_results AS results "
            "JOIN platform.pipeline_runs AS runs USING (run_id) "
            "WHERE results.source = %s AND results.dataset = %s "
            "AND runs.status = 'succeeded' AND results.table_rows IS NOT NULL "
            "ORDER BY results.checked_at DESC, results.position DESC LIMIT 1",
            [source, dataset],
        ).fetchone()
        return int(row[0]) if row else None

    def record_findings(self, run_id: UUID, findings: RunFindings, recorded_at: datetime) -> None:
        if findings.quarantine:
            copy_statement = (
                "COPY platform.quarantine "
                "(run_id, source, dataset, reason, record, quarantined_at) FROM STDIN"
            )
            with self._conn.cursor() as cursor, cursor.copy(copy_statement) as copy:
                for frame in findings.quarantine:
                    for reason, record in frame.iter_rows():
                        copy.write_row(
                            [run_id, findings.source, findings.dataset, reason, record, recorded_at]
                        )
        if findings.results:
            with self._conn.cursor() as cursor:
                cursor.executemany(
                    "INSERT INTO platform.quality_results (run_id, source, dataset, position, "
                    "check_type, columns, severity, passed, failing_rows, table_rows, message, "
                    "settings, checked_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    [
                        [
                            run_id,
                            findings.source,
                            findings.dataset,
                            result.position,
                            result.check_type,
                            list(result.columns),
                            result.severity,
                            result.passed,
                            result.failing_rows,
                            result.table_rows,
                            result.message,
                            Jsonb(result.settings),
                            recorded_at,
                        ]
                        for result in findings.results
                    ],
                )
        self._conn.execute(
            "UPDATE platform.pipeline_runs SET rows_quarantined = %s WHERE run_id = %s",
            [findings.quarantined_rows, run_id],
        )

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
