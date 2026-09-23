"""Every read the API makes, as plain SQL against the platform database."""

import math
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime, time
from decimal import Decimal
from threading import Lock
from typing import Any
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from udp import __version__
from udp.api.metrics import LastRun, MetricsSnapshot, QualityFailures, RunTotals
from udp.api.models import (
    CheckResult,
    Column,
    DatasetDetail,
    DatasetItem,
    DatasetProfile,
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
from udp.api.profile import PROFILE_ROW_LIMIT, profile_table


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
           d.schedule, d.definition, d.recorded_at, s.connector_type,
           r.run_id AS last_run_id, r.status AS last_status, r.trigger AS last_trigger,
           r.started_at AS last_started_at, r.ended_at AS last_ended_at,
           r.rows_loaded AS last_rows_loaded
    FROM platform.datasets AS d
    JOIN platform.sources AS s USING (source)
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


def _table_rows(conn: psycopg.Connection[dict[str, Any]], table_name: str) -> int | None:
    """The exact number of rows in a dataset's table, or None when it does not exist yet."""
    exists = conn.execute(
        "SELECT to_regclass(%s) IS NOT NULL AS found", [f"datasets.{table_name}"]
    ).fetchone()
    if exists is None or not exists["found"]:
        return None
    row = conn.execute(
        sql.SQL("SELECT count(*) AS n FROM {}").format(sql.Identifier("datasets", table_name))
    ).fetchone()
    return int(row["n"]) if row else None


def _dataset_item(row: dict[str, Any], table_rows: int | None) -> DatasetItem:
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
        connector_type=row["connector_type"],
        table_name=row["table_name"],
        load_mode=row["load_mode"],
        schedule=row["schedule"],
        recorded_at=row["recorded_at"],
        table_rows=table_rows,
        last_run=last_run,
    )


def _columns(pairs: list[list[str]]) -> list[Column]:
    return [Column(name=name, type=kind) for name, kind in pairs]


POOL_SIZE = 8
POOL_WAIT_SECONDS = 5.0
POOL_IDLE_SECONDS = 30.0


def _utc(conn: psycopg.Connection[dict[str, Any]]) -> None:
    """Every connection reads and writes times in UTC, however the server is set up."""
    conn.execute("SET TIME ZONE 'UTC'")


class PostgresCatalog:
    """Reads through a small pool, opened on the first call and closed with the app.

    Opening it lazily keeps `udp openapi` and the tests that never touch a database from
    connecting to anything at all.
    """

    def __init__(self, database_url: str) -> None:
        self._database_url = database_url
        self._pool: ConnectionPool[psycopg.Connection[dict[str, Any]]] | None = None
        self._opening = Lock()

    def _ready(self) -> ConnectionPool[psycopg.Connection[dict[str, Any]]]:
        with self._opening:
            if self._pool is None:
                self._pool = ConnectionPool(
                    self._database_url,
                    # Nothing is held while nothing is asked for: an idle API keeps no
                    # connection, and a machine full of them has none to spare.
                    min_size=0,
                    max_size=POOL_SIZE,
                    max_idle=POOL_IDLE_SECONDS,
                    timeout=POOL_WAIT_SECONDS,
                    open=False,
                    kwargs={"autocommit": True, "connect_timeout": 3, "row_factory": dict_row},
                    configure=_utc,
                )
                self._pool.open()
            return self._pool

    def close(self) -> None:
        """Give back every connection; the next read opens the pool again."""
        with self._opening:
            pool, self._pool = self._pool, None
        if pool is not None:
            pool.close()

    @contextmanager
    def _connect(self) -> Iterator[psycopg.Connection[dict[str, Any]]]:
        with self._ready().connection() as conn:
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
            return [
                _dataset_item(row, _table_rows(conn, row["table_name"]))
                for row in self._dataset_rows(conn, q, source)
            ]

    def source(self, name: str) -> SourceDetail | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT source, connector_type, connection, recorded_at FROM platform.sources "
                "WHERE source = %s",
                [name],
            ).fetchone()
            if row is None:
                return None
            datasets = [
                _dataset_item(item, _table_rows(conn, item["table_name"]))
                for item in self._dataset_rows(conn, None, name)
            ]
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
            item = _dataset_item(row, _table_rows(conn, row["table_name"]))
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

    def profile(
        self, source: str, dataset: str, row_limit: int = PROFILE_ROW_LIMIT
    ) -> DatasetProfile | None:
        with self._connect() as conn:
            found = conn.execute(
                "SELECT table_name FROM platform.datasets WHERE source = %s AND dataset = %s",
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
                return DatasetProfile(table_rows=0, profiled_rows=0, sampled=False, columns=[])
            return profile_table(
                conn,
                found["table_name"],
                [(column["name"], column["type"]) for column in columns],
                json_value,
                row_limit,
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

    def metrics(self) -> MetricsSnapshot:
        with self._connect() as conn:
            totals = conn.execute(
                "SELECT source, dataset, status, count(*) AS runs, "
                "coalesce(sum(rows_extracted), 0) AS rows_extracted, "
                "coalesce(sum(rows_loaded), 0) AS rows_loaded, "
                "coalesce(sum(rows_quarantined), 0) AS rows_quarantined "
                "FROM platform.pipeline_runs GROUP BY source, dataset, status"
            ).fetchall()
            last_runs = conn.execute(
                "SELECT DISTINCT ON (source, dataset) source, dataset, status, started_at, "
                "ended_at FROM platform.pipeline_runs "
                "ORDER BY source, dataset, started_at DESC, run_id DESC"
            ).fetchall()
            # Read on its own: as a column of the query above it would run once per stored run.
            succeeded = {
                (row["source"], row["dataset"]): row["ended_at"]
                for row in conn.execute(
                    "SELECT source, dataset, max(ended_at) AS ended_at "
                    "FROM platform.pipeline_runs WHERE status = 'succeeded' "
                    "GROUP BY source, dataset"
                ).fetchall()
            }
            # The same run the quality tab shows: each dataset's newest run that has results.
            quality = conn.execute(
                "SELECT results.source, results.dataset, results.severity, "
                "count(*) FILTER (WHERE NOT results.passed) AS failed "
                "FROM platform.quality_results AS results JOIN ("
                "  SELECT DISTINCT ON (checked.source, checked.dataset) checked.run_id "
                "  FROM platform.quality_results AS checked "
                "  JOIN platform.pipeline_runs AS runs USING (run_id) "
                "  ORDER BY checked.source, checked.dataset, runs.started_at DESC, runs.run_id DESC"
                ") AS newest USING (run_id) "
                "GROUP BY results.source, results.dataset, results.severity"
            ).fetchall()
        return MetricsSnapshot(
            totals=tuple(
                RunTotals(
                    source=row["source"],
                    dataset=row["dataset"],
                    status=row["status"],
                    runs=row["runs"],
                    rows_extracted=int(row["rows_extracted"]),
                    rows_loaded=int(row["rows_loaded"]),
                    rows_quarantined=int(row["rows_quarantined"]),
                )
                for row in totals
            ),
            last_runs=tuple(
                LastRun(**row, succeeded_at=succeeded.get((row["source"], row["dataset"])))
                for row in last_runs
            ),
            quality=tuple(QualityFailures(**row) for row in quality),
            version=__version__,
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
