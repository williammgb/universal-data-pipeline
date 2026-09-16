"""Every read the API makes, as plain SQL against the platform database."""

import math
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from udp.api.models import (
    CheckResult,
    Column,
    DatasetDetail,
    DatasetItem,
    JsonValue,
    QualityReport,
    RowsPage,
    RunDetail,
    RunItem,
    RunsPage,
    RunSummary,
    SavedState,
    SchemaVersion,
    SourceDetail,
    SourceItem,
)


def json_value(value: Any) -> JsonValue:
    """A stored value as JSON: exact decimals and non-finite floats become text."""
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, date | time):  # datetime is a date
        return value.isoformat()
    return str(value)  # UUID, and any other stored type, as its text form


_DATASET_ITEMS = """
    SELECT d.source, d.dataset, d.table_name, d.load_mode, d.primary_key, d.watermark,
           d.schedule, d.definition, d.recorded_at,
           r.run_id AS last_run_id, r.status AS last_status, r.trigger AS last_trigger,
           r.started_at AS last_started_at, r.ended_at AS last_ended_at,
           r.rows_loaded AS last_rows_loaded
    FROM platform.datasets AS d
    LEFT JOIN LATERAL (
        SELECT run_id, status, trigger, started_at, ended_at, rows_loaded
        FROM platform.pipeline_runs AS p
        WHERE p.source = d.source AND p.dataset = d.dataset
        ORDER BY p.started_at DESC, p.run_id DESC
        LIMIT 1
    ) AS r ON true
"""

_RUN_COLUMNS = (
    "run_id, source, dataset, trigger, status, started_at, ended_at, rows_extracted, "
    "rows_loaded, rows_quarantined, error_class, error_message"
)


def _dataset_item(row: dict[str, Any]) -> DatasetItem:
    last_run = None
    if row["last_run_id"] is not None:
        last_run = RunSummary(
            run_id=row["last_run_id"],
            status=row["last_status"],
            trigger=row["last_trigger"],
            started_at=row["last_started_at"],
            ended_at=row["last_ended_at"],
            rows_loaded=row["last_rows_loaded"],
        )
    return DatasetItem(
        source=row["source"],
        dataset=row["dataset"],
        table_name=row["table_name"],
        load_mode=row["load_mode"],
        schedule=row["schedule"],
        recorded_at=row["recorded_at"],
        last_run=last_run,
    )


def _columns(pairs: list[list[str]]) -> list[Column]:
    return [Column(name=name, type=kind) for name, kind in pairs]


class PostgresCatalog:
    """Opens one connection per call, so a request never waits on another's connection."""

    def __init__(self, database_url: str) -> None:
        self._database_url = database_url

    @contextmanager
    def _connect(self) -> Iterator[psycopg.Connection[dict[str, Any]]]:
        with psycopg.connect(
            self._database_url, autocommit=True, connect_timeout=3, row_factory=dict_row
        ) as conn:
            conn.execute("SET TIME ZONE 'UTC'")
            yield conn

    def ping(self) -> None:
        with self._connect() as conn:
            conn.execute("SELECT 1")

    def sources(self, q: str | None) -> list[SourceItem]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT s.source, s.connector_type, s.recorded_at, count(d.dataset) AS datasets "
                "FROM platform.sources AS s LEFT JOIN platform.datasets AS d USING (source) "
                "WHERE %(q)s::text IS NULL OR strpos(lower(s.source), lower(%(q)s)) > 0 "
                "GROUP BY s.source ORDER BY s.source",
                {"q": q},
            ).fetchall()
        return [SourceItem.model_validate(row) for row in rows]

    def _dataset_rows(
        self, conn: psycopg.Connection[dict[str, Any]], q: str | None, source: str | None
    ) -> list[dict[str, Any]]:
        return conn.execute(
            _DATASET_ITEMS + "WHERE (%(source)s::text IS NULL OR d.source = %(source)s) "
            "AND (%(q)s::text IS NULL OR strpos(lower(d.source), lower(%(q)s)) > 0 "
            "OR strpos(lower(d.dataset), lower(%(q)s)) > 0) "
            "ORDER BY d.source, d.dataset",
            {"q": q, "source": source},
        ).fetchall()

    def datasets(self, q: str | None, source: str | None) -> list[DatasetItem]:
        with self._connect() as conn:
            return [_dataset_item(row) for row in self._dataset_rows(conn, q, source)]

    def source(self, name: str) -> SourceDetail | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT source, connector_type, connection, recorded_at FROM platform.sources "
                "WHERE source = %s",
                [name],
            ).fetchone()
            if row is None:
                return None
            datasets = [_dataset_item(item) for item in self._dataset_rows(conn, None, name)]
        return SourceDetail(**row, datasets=datasets)

    def dataset(self, source: str, dataset: str) -> DatasetDetail | None:
        with self._connect() as conn:
            row = conn.execute(
                _DATASET_ITEMS + "WHERE d.source = %s AND d.dataset = %s", [source, dataset]
            ).fetchone()
            if row is None:
                return None
            versions = conn.execute(
                "SELECT version, run_id, recorded_at, columns FROM platform.schema_versions "
                "WHERE source = %s AND dataset = %s ORDER BY version",
                [source, dataset],
            ).fetchall()
            state = conn.execute(
                "SELECT watermark_column, watermark_type, watermark, file_path, file_sha256, "
                "run_id, saved_at FROM platform.source_state WHERE source = %s AND dataset = %s",
                [source, dataset],
            ).fetchone()
        item = _dataset_item(row)
        return DatasetDetail(
            **item.model_dump(),
            primary_key=list(row["primary_key"]),
            watermark=row["watermark"],
            definition=row["definition"],
            columns=_columns(versions[-1]["columns"]) if versions else [],
            versions=[
                SchemaVersion(
                    version=version["version"],
                    run_id=version["run_id"],
                    recorded_at=version["recorded_at"],
                    columns=_columns(version["columns"]),
                )
                for version in versions
            ],
            state=SavedState.model_validate(state) if state is not None else None,
        )

    def rows(self, source: str, dataset: str, limit: int, offset: int) -> RowsPage | None:
        with self._connect() as conn:
            found = conn.execute(
                "SELECT table_name, primary_key FROM platform.datasets "
                "WHERE source = %s AND dataset = %s",
                [source, dataset],
            ).fetchone()
            if found is None:
                return None
            columns = conn.execute(
                "SELECT attname AS name, format_type(atttypid, atttypmod) AS type "
                "FROM pg_attribute WHERE attrelid = to_regclass(%s) AND attnum > 0 "
                "AND NOT attisdropped ORDER BY attnum",
                [f"datasets.{found['table_name']}"],
            ).fetchall()
            if not columns:
                return RowsPage(columns=[], rows=[], limit=limit, offset=offset, has_more=False)
            # A table without a key is paged in physical order, which holds while it is unchanged.
            order = (
                sql.SQL(", ").join(sql.Identifier(name) for name in found["primary_key"])
                if found["primary_key"]
                else sql.SQL("ctid")
            )
            rows = conn.execute(
                sql.SQL("SELECT * FROM {} ORDER BY {} LIMIT %s OFFSET %s").format(
                    sql.Identifier("datasets", found["table_name"]), order
                ),
                [limit + 1, offset],
            ).fetchall()
        return RowsPage(
            columns=[Column.model_validate(column) for column in columns],
            rows=[{name: json_value(value) for name, value in row.items()} for row in rows[:limit]],
            limit=limit,
            offset=offset,
            has_more=len(rows) > limit,
        )

    def quality(self, source: str, dataset: str) -> QualityReport | None:
        with self._connect() as conn:
            known = conn.execute(
                "SELECT 1 FROM platform.datasets WHERE source = %s AND dataset = %s",
                [source, dataset],
            ).fetchone()
            if known is None:
                return None
            newest = conn.execute(
                "SELECT results.run_id FROM platform.quality_results AS results "
                "JOIN platform.pipeline_runs AS runs USING (run_id) "
                "WHERE results.source = %s AND results.dataset = %s "
                "ORDER BY runs.started_at DESC, runs.run_id DESC LIMIT 1",
                [source, dataset],
            ).fetchone()
            if newest is None:
                return QualityReport(run_id=None, results=[])
            results = self._results(conn, newest["run_id"])
        return QualityReport(run_id=newest["run_id"], results=results)

    @staticmethod
    def _results(conn: psycopg.Connection[dict[str, Any]], run_id: UUID) -> list[CheckResult]:
        rows = conn.execute(
            "SELECT position, check_type, columns, severity, passed, failing_rows, table_rows, "
            "message, settings, checked_at FROM platform.quality_results "
            "WHERE run_id = %s ORDER BY position",
            [run_id],
        ).fetchall()
        return [CheckResult.model_validate(row) for row in rows]

    def runs(
        self,
        *,
        source: str | None = None,
        dataset: str | None = None,
        status: str | None = None,
        trigger: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> RunsPage:
        conditions: list[sql.Composable] = []
        values: list[Any] = []
        for column, value in (
            ("source", source),
            ("dataset", dataset),
            ("status", status),
            ("trigger", trigger),
        ):
            if value is not None:
                conditions.append(sql.SQL("{} = %s").format(sql.Identifier(column)))
                values.append(value)
        if since is not None:
            conditions.append(sql.SQL("started_at >= %s"))
            values.append(since)
        if until is not None:
            conditions.append(sql.SQL("started_at < %s"))
            values.append(until)
        where = (
            sql.SQL(" WHERE ") + sql.SQL(" AND ").join(conditions) if conditions else sql.SQL("")
        )
        query = sql.SQL(
            "SELECT {} FROM platform.pipeline_runs{} "
            "ORDER BY started_at DESC, run_id DESC LIMIT %s OFFSET %s"
        ).format(sql.SQL(_RUN_COLUMNS), where)
        with self._connect() as conn:
            rows = conn.execute(query, [*values, limit + 1, offset]).fetchall()
        return RunsPage(
            runs=[RunItem.model_validate(row) for row in rows[:limit]],
            limit=limit,
            offset=offset,
            has_more=len(rows) > limit,
        )

    def run(self, run_id: UUID) -> RunDetail | None:
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT {_RUN_COLUMNS}, error_traceback FROM platform.pipeline_runs "
                "WHERE run_id = %s",
                [run_id],
            ).fetchone()
            if row is None:
                return None
            quality = self._results(conn, run_id)
        return RunDetail(**row, quality=quality)
