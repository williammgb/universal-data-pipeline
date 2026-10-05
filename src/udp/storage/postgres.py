import json
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from datetime import date, datetime
from itertools import chain
from typing import Any, Self
from uuid import UUID

import polars as pl
import psycopg
import structlog
from psycopg import sql
from psycopg.types.json import Jsonb

from udp.config.columns import is_json, json_text
from udp.connectors.base import DatasetBase
from udp.errors import LoadError, SchemaDriftError
from udp.names import Stage, stage_table
from udp.profiling.frame import ProfileSettings
from udp.profiling.models import ProfileComparison, StageProfile, compare_profiles
from udp.profiling.stage import READ_BATCH, profile_stage, read_as, stage_columns
from udp.profiling.table import PROFILE_ROW_LIMIT
from udp.quality.constraints import check_stage
from udp.storage import overrides as override_store
from udp.storage.loader import (
    INTERRUPTED,
    Column,
    ColumnChanges,
    ConfigCopy,
    ConstraintResult,
    DatasetState,
    Execution,
    ExecutionStart,
    LineageNode,
    LoadResult,
    PipelineVersion,
    Profile,
    RunFailure,
    RunFindings,
    RunRef,
    RunStart,
    StepDefinition,
    StepRun,
    StoredProfile,
    Violation,
    check_chain,
    column_changes,
    interrupted_message,
    table_columns,
    watermark_from_text,
    watermark_to_text,
)

log = structlog.get_logger(step="run")

_STAGE_TABLE = "udp_stage"
_STAGE = sql.Identifier(_STAGE_TABLE)
_STAGE_ROW_COLUMN = "_stage_row"
_STAGE_ROW = sql.Identifier(_STAGE_ROW_COLUMN)
_RAW_GUARD = sql.Identifier("raw_append_only")


def _guard_raw(conn: psycopg.Connection, target: sql.Identifier) -> None:
    """Make a new RAW table refuse every UPDATE, DELETE and TRUNCATE from now on."""
    conn.execute(
        sql.SQL(
            "CREATE TRIGGER {} BEFORE UPDATE OR DELETE OR TRUNCATE ON {} "
            "FOR EACH STATEMENT EXECUTE FUNCTION platform.refuse_raw_change()"
        ).format(_RAW_GUARD, target)
    )


def _identifiers(names: Sequence[str]) -> sql.Composed:
    return sql.SQL(", ").join(sql.Identifier(name) for name in names)


class PostgresTransaction:
    def __init__(self, conn: psycopg.Connection) -> None:
        self._conn = conn

    def _columns(self, table: str, schema: str = "datasets") -> list[Column]:
        rows = self._conn.execute(
            "SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute "
            "WHERE attrelid = to_regclass(%s) AND attnum > 0 AND NOT attisdropped "
            "ORDER BY attnum",
            [f"{schema}.{table}"],
        ).fetchall()
        return [(name, kind) for name, kind in rows]

    def _stage(
        self,
        table: str,
        chunks: Iterable[pl.DataFrame],
        primary_key: Sequence[str] = (),
        numbered: bool = False,
        schema: str = "datasets",
    ) -> tuple[list[str], ColumnChanges]:
        """Create or widen the table, then COPY every chunk into a temporary stage table."""
        remaining = iter(chunks)
        first = next(remaining, None)
        if first is None:
            raise LoadError(f"no data to load into {schema}.{table}")
        names = first.columns
        target = sql.Identifier(schema, table)

        existing = self._columns(table, schema)
        if existing:
            changes = column_changes(table, existing, first.schema, schema)
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
                # A JSON column goes in as its text, which the jsonb column parses.
                as_text = chunk.with_columns(
                    json_text(pl.col(name)).alias(name)
                    for name, dtype in chunk.schema.items()
                    if is_json(dtype)
                )
                copy.write(as_text.write_csv(include_header=False, quote_style="non_numeric"))
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

    def drop_raw(self, source: str, dataset: str) -> None:
        # The guard refuses UPDATE, DELETE and TRUNCATE; dropping the table is how a full
        # refresh, and nothing else, starts RAW over.
        self._conn.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(_stage_identifier(Stage.RAW, source, dataset))
        )

    def append_raw(self, source: str, dataset: str) -> int:
        # The stage table still holds the load's rows, typed as the dataset table types them, so
        # RAW gets exactly what the load read without reading the source twice.
        schema, table = stage_table(Stage.RAW, source, dataset)
        target = sql.Identifier(schema, table)
        loaded = [
            (name, kind)
            for name, kind in self._columns(_STAGE_TABLE, "pg_temp")
            if name != _STAGE_ROW_COLUMN
        ]
        if not loaded:
            raise LoadError(f"nothing was loaded for {source}.{dataset} to add to RAW")
        stored = dict(self._columns(table, schema))
        if stored:
            for name, kind in loaded:
                if name not in stored:
                    self._conn.execute(
                        sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                            target, sql.Identifier(name), sql.SQL(kind)
                        )
                    )
                elif kind != stored[name]:
                    raise SchemaDriftError(
                        f"column '{name}' of {schema}.{table} is {stored[name]} but this run "
                        f"loaded it as {kind}; RAW keeps what was ingested, so run with "
                        "--full-refresh to start it over"
                    )
        else:
            definition = [
                sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(kind))
                for name, kind in loaded
            ]
            self._conn.execute(
                sql.SQL("CREATE TABLE {} ({})").format(target, sql.SQL(", ").join(definition))
            )
            _guard_raw(self._conn, target)
        names = [name for name, _ in loaded]
        inserted = self._conn.execute(
            sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                target, _identifiers(names), _identifiers(names), _STAGE
            )
        )
        return inserted.rowcount

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


def _stage_identifier(stage: Stage, source: str, dataset: str) -> sql.Identifier:
    return sql.Identifier(*stage_table(stage, source, dataset))


def _stage_value(stage: Stage | None) -> str | None:
    """The stage as the text the tables hold; never the enum, whose adaptation is psycopg's call."""
    return None if stage is None else Stage(stage).value


def _run_values(run: RunRef) -> list[UUID | None]:
    return [run.ingest_run_id, run.execution_id]


def _run_filter(run: RunRef) -> sql.Composed:
    column = "ingest_run_id" if run.ingest_run_id is not None else "execution_id"
    return sql.SQL("{} = %s").format(sql.Identifier(column))


class PostgresStages:
    """A dataset's RAW, STAGING and CLEAN tables, and the V2 records, inside one transaction.

    The caller holds the dataset's lock (`PostgresLoader.lock_dataset`): STAGING is one table per
    dataset, so two executions over one dataset at once would share it.
    """

    def __init__(self, conn: psycopg.Connection) -> None:
        self._conn = conn
        self._tables = PostgresTransaction(conn)

    def exists(self, stage: Stage, source: str, dataset: str) -> bool:
        row = self._conn.execute(
            "SELECT to_regclass(%s) IS NOT NULL", [".".join(stage_table(stage, source, dataset))]
        ).fetchone()
        return bool(row and row[0])

    def append_raw(self, source: str, dataset: str, chunks: Iterable[pl.DataFrame]) -> LoadResult:
        """Add one ingest's rows, platform columns included, to RAW. The table is created on first
        use with a trigger that refuses every UPDATE, DELETE and TRUNCATE from then on."""
        schema, table = stage_table(Stage.RAW, source, dataset)
        created = not self.exists(Stage.RAW, source, dataset)
        names, changes = self._tables._stage(table, chunks, schema=schema)
        if "_run_id" not in names:
            raise LoadError(f"rows for {schema}.{table} must say which run ingested them (_run_id)")
        target = sql.Identifier(schema, table)
        if created:
            _guard_raw(self._conn, target)
        inserted = self._conn.execute(
            sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                target, _identifiers(names), _identifiers(names), _STAGE
            )
        )
        return LoadResult(inserted.rowcount, changes.added, changes.missing)

    def start_staging(
        self, source: str, dataset: str, *, ingest_runs: Sequence[UUID] | None = None
    ) -> int:
        """Make STAGING a fresh copy of RAW: of the rows the named ingest runs added, or of every
        row. Returns the rows copied. A STAGING table left from before is replaced."""
        if not self.exists(Stage.RAW, source, dataset):
            raise LoadError(f"{source}.{dataset} has no RAW table to copy into STAGING")
        raw = _stage_identifier(Stage.RAW, source, dataset)
        staging = _stage_identifier(Stage.STAGING, source, dataset)
        self._conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(staging))
        self._conn.execute(sql.SQL("CREATE TABLE {} (LIKE {})").format(staging, raw))
        copy = sql.SQL("INSERT INTO {} SELECT * FROM {}").format(staging, raw)
        if ingest_runs is None:
            return self._conn.execute(copy).rowcount
        chosen = copy + sql.SQL(" WHERE _run_id = ANY(%s)")
        return self._conn.execute(chosen, [list(ingest_runs)]).rowcount

    def read_raw(
        self, source: str, dataset: str, ingest_runs: Sequence[UUID] | None = None
    ) -> pl.DataFrame:
        """RAW's rows as a frame, platform columns included: of the named ingest runs, or every
        row. RAW is only read. Each column becomes the type `read_as` gives it — a jsonb or uuid
        column its text. Raises LoadError when there is no RAW table."""
        schema, table = stage_table(Stage.RAW, source, dataset)
        types = stage_columns(self._conn, schema, table)
        if not types:
            raise LoadError(f"{source}.{dataset} has no RAW table: run `udp run {source}` first")
        reads = [read_as(name, kind) for name, kind in types]
        frame_schema = pl.Schema(
            {name: dtype for (name, _), (_, dtype) in zip(types, reads, strict=True)}
        )
        query = sql.SQL("SELECT {} FROM {}").format(
            sql.SQL(", ").join(expression for expression, _ in reads), sql.Identifier(schema, table)
        )
        arguments: list[Any] = []
        if ingest_runs is not None:
            query += sql.SQL(" WHERE _run_id = ANY(%s)")
            arguments.append(list(ingest_runs))
        batches = [pl.DataFrame(schema=frame_schema)]
        with self._conn.cursor(name="udp_raw_read") as cursor:
            cursor.itersize = READ_BATCH
            cursor.execute(query, arguments)
            while rows := cursor.fetchmany(READ_BATCH):
                batches.append(pl.DataFrame(rows, schema=frame_schema, orient="row"))
        frame = pl.concat(batches, how="vertical")
        wide = [
            pl.col(name).cast(pl.Float64, strict=False)
            for name, kind in types
            if kind.startswith("numeric") and isinstance(frame_schema[name], pl.String)
        ]
        return frame.with_columns(wide)

    def write_staging(self, source: str, dataset: str, frame: pl.DataFrame) -> None:
        """Make STAGING exactly this frame, its columns typed as the loader types them,
        replacing any STAGING table there was."""
        if not frame.columns:
            raise LoadError(f"nothing to write to STAGING of {source}.{dataset}: no columns")
        schema, table = stage_table(Stage.STAGING, source, dataset)
        target = sql.Identifier(schema, table)
        self._conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(target))
        names, _ = self._tables._stage(table, [frame], schema=schema)
        self._conn.execute(
            sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                target, _identifiers(names), _identifiers(names), _STAGE
            )
        )

    def running_executions(self, source: str, dataset: str) -> list[UUID]:
        """The executions over the dataset still marked running, oldest first."""
        rows = self._conn.execute(
            "SELECT runs.execution_id FROM platform.pipeline_executions AS runs "
            "JOIN platform.pipelines AS pipelines USING (pipeline_id) "
            "WHERE pipelines.source = %s AND pipelines.dataset = %s AND runs.status = 'running' "
            "ORDER BY runs.started_at, runs.execution_id",
            [source, dataset],
        ).fetchall()
        return [row[0] for row in rows]

    def read_pipeline_version(self, pipeline_id: int, version: int) -> PipelineVersion | None:
        """One version of a pipeline, by the pipeline's id."""
        row = self._conn.execute(
            "SELECT source, dataset, name FROM platform.pipelines WHERE pipeline_id = %s",
            [pipeline_id],
        ).fetchone()
        if row is None:
            return None
        source, dataset, name = row
        found = self.read_pipeline(source, dataset, name, version)
        return found if found is not None and found.version == version else None

    def save_pipeline(
        self,
        source: str,
        dataset: str,
        name: str,
        definition: dict[str, Any],
        steps: Sequence[StepDefinition],
        created_at: datetime,
    ) -> PipelineVersion:
        """Store a pipeline definition. When it equals the newest version, that version is
        returned unchanged; otherwise it becomes the next version, and older ones stay as they are.
        """
        stage_table(Stage.CLEAN, source, dataset)
        if not name:
            raise ValueError("a pipeline needs a name")
        newest = self.read_pipeline(source, dataset, name)
        if (
            newest is not None
            and newest.definition == json.loads(json.dumps(definition))
            and newest.steps == tuple(_round_trip(step) for step in steps)
        ):
            return newest
        self._conn.execute(
            "INSERT INTO platform.pipelines (source, dataset, name, created_at) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (source, dataset, name) DO NOTHING",
            [source, dataset, name, created_at],
        )
        row = self._conn.execute(
            "SELECT pipeline_id FROM platform.pipelines "
            "WHERE source = %s AND dataset = %s AND name = %s",
            [source, dataset, name],
        ).fetchone()
        assert row is not None
        pipeline_id = int(row[0])
        version = 1 if newest is None else newest.version + 1
        self._conn.execute(
            "INSERT INTO platform.pipeline_versions (pipeline_id, version, definition, created_at) "
            "VALUES (%s, %s, %s, %s)",
            [pipeline_id, version, Jsonb(definition), created_at],
        )
        with self._conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO platform.pipeline_steps "
                "(pipeline_id, version, position, step_type, configuration) "
                "VALUES (%s, %s, %s, %s, %s)",
                [
                    [pipeline_id, version, position, step.step_type, Jsonb(step.configuration)]
                    for position, step in enumerate(steps, start=1)
                ],
            )
        saved = self.read_pipeline(source, dataset, name, version)
        assert saved is not None
        return saved

    def read_pipeline(
        self, source: str, dataset: str, name: str, version: int | None = None
    ) -> PipelineVersion | None:
        """One version of a pipeline, the newest when no version is given."""
        row = self._conn.execute(
            "SELECT pipelines.pipeline_id, versions.version, versions.definition, "
            "versions.created_at FROM platform.pipelines AS pipelines "
            "JOIN platform.pipeline_versions AS versions USING (pipeline_id) "
            "WHERE pipelines.source = %s AND pipelines.dataset = %s AND pipelines.name = %s "
            "AND (%s::integer IS NULL OR versions.version = %s) "
            "ORDER BY versions.version DESC LIMIT 1",
            [source, dataset, name, version, version],
        ).fetchone()
        if row is None:
            return None
        pipeline_id, found, definition, created_at = row
        steps = self._conn.execute(
            "SELECT step_type, configuration FROM platform.pipeline_steps "
            "WHERE pipeline_id = %s AND version = %s ORDER BY position",
            [pipeline_id, found],
        ).fetchall()
        return PipelineVersion(
            pipeline_id=pipeline_id,
            source=source,
            dataset=dataset,
            name=name,
            version=found,
            definition=definition,
            steps=tuple(StepDefinition(kind, configuration) for kind, configuration in steps),
            created_at=created_at,
        )

    def start_execution(self, start: ExecutionStart) -> None:
        self._conn.execute(
            "INSERT INTO platform.pipeline_executions "
            "(execution_id, pipeline_id, version, trigger, status, started_at) "
            "VALUES (%s, %s, %s, %s, 'running', %s)",
            [start.execution_id, start.pipeline_id, start.version, start.trigger, start.started_at],
        )

    def record_step(self, execution_id: UUID, step: StepRun) -> None:
        """Write a step's record, or update it while it is still running."""
        known = self._conn.execute(
            "SELECT 1 FROM platform.pipeline_executions AS runs "
            "JOIN platform.pipeline_steps AS steps USING (pipeline_id, version) "
            "WHERE runs.execution_id = %s AND steps.position = %s",
            [execution_id, step.position],
        ).fetchone()
        if known is None:
            raise LoadError(f"execution {execution_id} ran no pipeline with a step {step.position}")
        written = self._conn.execute(
            "INSERT INTO platform.step_executions (execution_id, position, status, started_at, "
            "ended_at, rows_in, rows_out, values_changed, error_class, error_message, "
            "script_sha256, output, error_line) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (execution_id, position) DO UPDATE SET status = EXCLUDED.status, "
            "started_at = EXCLUDED.started_at, ended_at = EXCLUDED.ended_at, "
            "rows_in = EXCLUDED.rows_in, rows_out = EXCLUDED.rows_out, "
            "values_changed = EXCLUDED.values_changed, error_class = EXCLUDED.error_class, "
            "error_message = EXCLUDED.error_message, script_sha256 = EXCLUDED.script_sha256, "
            "output = EXCLUDED.output, error_line = EXCLUDED.error_line "
            "WHERE step_executions.status = 'running'",
            [
                execution_id,
                step.position,
                step.status,
                step.started_at,
                step.ended_at,
                step.rows_in,
                step.rows_out,
                step.values_changed,
                step.error_class,
                step.error_message,
                step.script_sha256,
                step.output,
                step.error_line,
            ],
        )
        if written.rowcount != 1:
            raise LoadError(f"step {step.position} of execution {execution_id} has already ended")

    def finish_execution(
        self,
        execution_id: UUID,
        *,
        ended_at: datetime,
        rows_in: int | None,
        rows_out: int | None,
        failure: RunFailure | None = None,
        failed_step: int | None = None,
    ) -> None:
        """End a running execution. Without a failure it succeeded, and its STAGING table becomes
        the dataset's CLEAN table, replacing the old one. With a failure STAGING is dropped and
        CLEAN stays exactly as it was."""
        if failed_step is not None and failure is None:
            raise ValueError("only a failed execution names the step that failed")
        row = self._conn.execute(
            "UPDATE platform.pipeline_executions AS runs SET status = %s, ended_at = %s, "
            "rows_in = %s, rows_out = %s, failed_step = %s, error_class = %s, "
            "error_message = %s, error_traceback = %s FROM platform.pipelines AS pipelines "
            "WHERE runs.execution_id = %s AND runs.status = 'running' "
            "AND pipelines.pipeline_id = runs.pipeline_id "
            "RETURNING pipelines.source, pipelines.dataset",
            [
                "succeeded" if failure is None else "failed",
                ended_at,
                rows_in,
                rows_out,
                failed_step,
                None if failure is None else failure.error_class,
                None if failure is None else failure.message,
                None if failure is None else failure.traceback,
                execution_id,
            ],
        ).fetchone()
        if row is None:
            raise LoadError(f"execution {execution_id} is not running")
        source, dataset = row
        staging = _stage_identifier(Stage.STAGING, source, dataset)
        if failure is not None:
            self._conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(staging))
            return
        if not self.exists(Stage.STAGING, source, dataset):
            raise LoadError(f"execution {execution_id} has no STAGING table to publish as CLEAN")
        clean_schema, _ = stage_table(Stage.CLEAN, source, dataset)
        self._conn.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(
                _stage_identifier(Stage.CLEAN, source, dataset)
            )
        )
        self._conn.execute(
            sql.SQL("ALTER TABLE {} SET SCHEMA {}").format(staging, sql.Identifier(clean_schema))
        )

    def read_execution(self, execution_id: UUID) -> Execution | None:
        row = self._conn.execute(
            "SELECT runs.pipeline_id, runs.version, pipelines.source, pipelines.dataset, "
            "runs.trigger, runs.status, runs.started_at, runs.ended_at, runs.rows_in, "
            "runs.rows_out, runs.failed_step, runs.error_class, runs.error_message "
            "FROM platform.pipeline_executions AS runs "
            "JOIN platform.pipelines AS pipelines USING (pipeline_id) "
            "WHERE runs.execution_id = %s",
            [execution_id],
        ).fetchone()
        if row is None:
            return None
        steps = self._conn.execute(
            "SELECT position, status, started_at, ended_at, rows_in, rows_out, values_changed, "
            "error_class, error_message, script_sha256, output, error_line "
            "FROM platform.step_executions "
            "WHERE execution_id = %s ORDER BY position",
            [execution_id],
        ).fetchall()
        (
            pipeline_id,
            version,
            source,
            dataset,
            trigger,
            status,
            started_at,
            ended_at,
            rows_in,
            rows_out,
            failed_step,
            error_class,
            error_message,
        ) = row
        return Execution(
            execution_id=execution_id,
            pipeline_id=pipeline_id,
            version=version,
            source=source,
            dataset=dataset,
            trigger=trigger,
            status=status,
            started_at=started_at,
            ended_at=ended_at,
            rows_in=rows_in,
            rows_out=rows_out,
            failed_step=failed_step,
            error_class=error_class,
            error_message=error_message,
            steps=tuple(StepRun(*step) for step in steps),
        )

    def record_profile(self, profile: Profile) -> int:
        row = self._conn.execute(
            "INSERT INTO platform.profiles (source, dataset, stage, after_step, ingest_run_id, "
            "execution_id, table_rows, result, profiled_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING profile_id",
            [
                profile.source,
                profile.dataset,
                Stage(profile.stage).value,
                profile.after_step,
                *_run_values(profile.run),
                profile.table_rows,
                Jsonb(profile.result),
                profile.profiled_at,
            ],
        ).fetchone()
        assert row is not None
        return int(row[0])

    def read_profiles(
        self, source: str, dataset: str, stage: Stage | None = None, run: RunRef | None = None
    ) -> list[StoredProfile]:
        """The dataset's profiles in the order they were taken, of one stage or of every stage,
        and of one run or of every run."""
        ingest_run_id, execution_id = (None, None) if run is None else _run_values(run)
        rows = self._conn.execute(
            "SELECT profile_id, stage, after_step, ingest_run_id, execution_id, table_rows, "
            "result, profiled_at FROM platform.profiles "
            "WHERE source = %s AND dataset = %s AND (%s::text IS NULL OR stage = %s) "
            "AND (%s::uuid IS NULL OR ingest_run_id = %s) "
            "AND (%s::uuid IS NULL OR execution_id = %s) "
            "ORDER BY profile_id",
            [
                source,
                dataset,
                *[_stage_value(stage)] * 2,
                *[ingest_run_id] * 2,
                *[execution_id] * 2,
            ],
        ).fetchall()
        return [
            StoredProfile(
                profile_id,
                Profile(
                    source=source,
                    dataset=dataset,
                    stage=Stage(found),
                    run=RunRef(ingest_run_id, execution_id),
                    table_rows=table_rows,
                    result=result,
                    profiled_at=profiled_at,
                    after_step=after_step,
                ),
            )
            for (
                profile_id,
                found,
                after_step,
                ingest_run_id,
                execution_id,
                table_rows,
                result,
                profiled_at,
            ) in rows
        ]

    def read_profile(self, profile_id: int) -> StoredProfile | None:
        row = self._conn.execute(
            "SELECT source, dataset, stage, after_step, ingest_run_id, execution_id, table_rows, "
            "result, profiled_at FROM platform.profiles WHERE profile_id = %s",
            [profile_id],
        ).fetchone()
        if row is None:
            return None
        source, dataset, stage, after_step, ingest_run_id, execution_id, rows, result, at = row
        return StoredProfile(
            profile_id,
            Profile(
                source=source,
                dataset=dataset,
                stage=Stage(stage),
                run=RunRef(ingest_run_id, execution_id),
                table_rows=rows,
                result=result,
                profiled_at=at,
                after_step=after_step,
            ),
        )

    def profile(
        self,
        stage: Stage,
        source: str,
        dataset: str,
        run: RunRef,
        *,
        profiled_at: datetime,
        settings: ProfileSettings | None = None,
        after_step: int | None = None,
        row_limit: int = PROFILE_ROW_LIMIT,
    ) -> StoredProfile:
        """Profile the dataset's table at this stage and store the profile, tagged with the run.
        Raises LoadError when the table does not exist."""
        result = profile_stage(self._conn, stage, source, dataset, settings, row_limit)
        profile = Profile(
            source=source,
            dataset=dataset,
            stage=Stage(stage),
            run=run,
            table_rows=result.table_rows,
            result=result.model_dump(mode="json"),
            profiled_at=profiled_at,
            after_step=after_step,
        )
        return StoredProfile(self.record_profile(profile), profile)

    def check_constraints(
        self,
        stage: Stage,
        source: str,
        dataset: DatasetBase,
        run: RunRef,
        *,
        checked_at: datetime,
        after_step: int | None = None,
    ) -> list[ConstraintResult]:
        """Check the dataset's constraints against its table at this stage and store each result,
        tagged with the run. The table is only read. Raises LoadError when it does not exist."""
        results = [
            outcome.result(source, dataset.name, Stage(stage), run, checked_at, after_step)
            for outcome in check_stage(self._conn, stage, source, dataset)
        ]
        self.record_constraint_results(results)
        return results

    def compare_profiles(self, before: int, after: int) -> ProfileComparison:
        """Rows, missing values, invalid values, outliers and duplicates of two stored profiles,
        before and after. Raises LookupError when either profile is not stored."""
        found = []
        for profile_id in (before, after):
            stored = self.read_profile(profile_id)
            if stored is None:
                raise LookupError(f"no profile {profile_id} is stored")
            found.append(StageProfile.model_validate(stored.profile.result))
        return compare_profiles(*found)

    def newest_run(self, stage: Stage, source: str, dataset: str) -> RunRef | None:
        """The run a profile of the stage taken now belongs to: for RAW the newest succeeded
        ingest run that added rows to it, for STAGING the execution running over it, and for
        CLEAN the newest execution that succeeded. None when no run has made the table."""
        if Stage(stage) is Stage.RAW:
            if not self.exists(Stage.RAW, source, dataset):
                return None
            row = self._conn.execute(
                sql.SQL(
                    "SELECT run_id FROM platform.pipeline_runs AS runs "
                    "WHERE source = %s AND dataset = %s AND status = 'succeeded' "
                    "AND EXISTS (SELECT 1 FROM {} WHERE _run_id = runs.run_id) "
                    "ORDER BY started_at DESC LIMIT 1"
                ).format(_stage_identifier(Stage.RAW, source, dataset)),
                [source, dataset],
            ).fetchone()
            return None if row is None else RunRef(ingest_run_id=row[0])
        row = self._conn.execute(
            "SELECT runs.execution_id FROM platform.pipeline_executions AS runs "
            "JOIN platform.pipelines AS pipelines USING (pipeline_id) "
            "WHERE pipelines.source = %s AND pipelines.dataset = %s AND runs.status = %s "
            "ORDER BY runs.started_at DESC LIMIT 1",
            [source, dataset, "running" if Stage(stage) is Stage.STAGING else "succeeded"],
        ).fetchone()
        return None if row is None else RunRef(execution_id=row[0])

    def record_constraint_results(self, results: Sequence[ConstraintResult]) -> None:
        for result in results:
            row = self._conn.execute(
                "INSERT INTO platform.constraint_results (source, dataset, stage, after_step, "
                "ingest_run_id, execution_id, position, constraint_type, columns, critical, "
                "passed, failing_rows, failing_values, message, settings, checked_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "RETURNING result_id",
                [
                    result.source,
                    result.dataset,
                    Stage(result.stage).value,
                    result.after_step,
                    *_run_values(result.run),
                    result.position,
                    result.constraint_type,
                    list(result.columns),
                    result.critical,
                    result.passed,
                    result.failing_rows,
                    result.failing_values,
                    result.message,
                    Jsonb(result.settings),
                    result.checked_at,
                ],
            ).fetchone()
            assert row is not None
            if result.violations:
                with self._conn.cursor() as cursor:
                    cursor.executemany(
                        "INSERT INTO platform.constraint_violations "
                        "(result_id, column_name, row_key, value) VALUES (%s, %s, %s, %s)",
                        [
                            [row[0], violation.column, Jsonb(violation.row_key), violation.value]
                            for violation in result.violations
                        ],
                    )

    def read_constraint_results(
        self, source: str, dataset: str, stage: Stage | None = None, run: RunRef | None = None
    ) -> list[ConstraintResult]:
        """The dataset's constraint results in the order they were recorded, of one stage or of
        every stage, and of one run or of every run."""
        ingest_run_id, execution_id = (None, None) if run is None else _run_values(run)
        rows = self._conn.execute(
            "SELECT result_id, stage, after_step, ingest_run_id, execution_id, position, "
            "constraint_type, columns, critical, passed, failing_rows, failing_values, message, "
            "settings, checked_at FROM platform.constraint_results "
            "WHERE source = %s AND dataset = %s AND (%s::text IS NULL OR stage = %s) "
            "AND (%s::uuid IS NULL OR ingest_run_id = %s) "
            "AND (%s::uuid IS NULL OR execution_id = %s) "
            "ORDER BY result_id",
            [
                source,
                dataset,
                *[_stage_value(stage)] * 2,
                *[ingest_run_id] * 2,
                *[execution_id] * 2,
            ],
        ).fetchall()
        violations: dict[int, list[Violation]] = {}
        for result_id, column, row_key, value in self._conn.execute(
            "SELECT result_id, column_name, row_key, value FROM platform.constraint_violations "
            "WHERE result_id = ANY(%s) ORDER BY violation_id",
            [[row[0] for row in rows]],
        ):
            violations.setdefault(result_id, []).append(Violation(column, row_key, value))
        return [
            ConstraintResult(
                source=source,
                dataset=dataset,
                stage=Stage(found),
                run=RunRef(ingest_run_id, execution_id),
                position=position,
                constraint_type=constraint_type,
                columns=tuple(columns),
                critical=critical,
                passed=passed,
                failing_rows=failing_rows,
                failing_values=failing_values,
                message=message,
                settings=settings,
                checked_at=checked_at,
                after_step=after_step,
                violations=tuple(violations.get(result_id, ())),
            )
            for (
                result_id,
                found,
                after_step,
                ingest_run_id,
                execution_id,
                position,
                constraint_type,
                columns,
                critical,
                passed,
                failing_rows,
                failing_values,
                message,
                settings,
                checked_at,
            ) in rows
        ]

    def record_lineage(
        self,
        source: str,
        dataset: str,
        run: RunRef,
        nodes: Sequence[LineageNode],
        recorded_at: datetime,
    ) -> None:
        """Add nodes to the end of the run's lineage, as the run reaches them."""
        recorded = self.read_lineage(run)
        check_chain(source, dataset, run, [*recorded, *nodes])
        with self._conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO platform.lineage (source, dataset, ingest_run_id, execution_id, "
                "position, node, name, step_position, recorded_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                [
                    [
                        source,
                        dataset,
                        *_run_values(run),
                        position,
                        node.kind,
                        node.name,
                        node.step_position,
                        recorded_at,
                    ]
                    for position, node in enumerate(nodes, start=len(recorded) + 1)
                ],
            )

    def read_lineage(self, run: RunRef) -> tuple[LineageNode, ...]:
        """The run's lineage, in the order the data passed through it."""
        rows = self._conn.execute(
            sql.SQL(
                "SELECT node, name, step_position FROM platform.lineage WHERE {} ORDER BY position"
            ).format(_run_filter(run)),
            [run.ingest_run_id or run.execution_id],
        ).fetchall()
        return tuple(LineageNode(kind, name, step) for kind, name, step in rows)


def _round_trip(step: StepDefinition) -> StepDefinition:
    """The step as it reads back from jsonb, so an unchanged definition compares equal."""
    return StepDefinition(step.step_type, json.loads(json.dumps(step.configuration)))


class PostgresLoader:
    """Writes to the platform database. Connects on first use.

    Dataset locks are session-level advisory locks on this loader's connection, so they are
    freed when the connection closes, including when the process dies.
    """

    def __init__(self, database_url: str) -> None:
        self._database_url = database_url
        self._conn: psycopg.Connection | None = None
        self._held: set[str] = set()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        self._held.clear()

    def _connection(self) -> psycopg.Connection:
        if self._conn is None:
            self._conn = psycopg.connect(self._database_url, autocommit=True)
        return self._conn

    def read_overrides(self, source: str) -> dict[str, dict[str, Any]]:
        return override_store.read_overrides(self._connection(), source)

    def lock_dataset(self, source: str, dataset: str) -> bool:
        key = f"{source}/{dataset}"
        if key in self._held:
            return False
        row = (
            self._connection()
            .execute("SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", [key])
            .fetchone()
        )
        acquired = bool(row and row[0])
        if acquired:
            self._held.add(key)
        return acquired

    def unlock_dataset(self, source: str, dataset: str) -> None:
        key = f"{source}/{dataset}"
        self._held.discard(key)
        try:
            row = (
                self._connection()
                .execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", [key])
                .fetchone()
            )
        except psycopg.Error as error:
            log.warning("could not release the dataset lock", lock=key, error=str(error))
            return
        if not (row and row[0]):
            log.warning("the dataset lock was not held", lock=key)

    def record_config(self, copy: ConfigCopy) -> None:
        conn = self._connection()
        with conn.transaction():
            conn.execute(
                "INSERT INTO platform.sources "
                "(source, connector_type, connection, run_id, recorded_at) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (source) DO UPDATE SET "
                "connector_type = EXCLUDED.connector_type, connection = EXCLUDED.connection, "
                "run_id = EXCLUDED.run_id, recorded_at = EXCLUDED.recorded_at",
                [
                    copy.source,
                    copy.connector_type,
                    Jsonb(copy.connection),
                    copy.run_id,
                    copy.recorded_at,
                ],
            )
            conn.execute(
                "INSERT INTO platform.datasets (source, dataset, table_name, load_mode, "
                "primary_key, watermark, schedule, definition, run_id, recorded_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (source, dataset) DO UPDATE SET "
                "table_name = EXCLUDED.table_name, load_mode = EXCLUDED.load_mode, "
                "primary_key = EXCLUDED.primary_key, watermark = EXCLUDED.watermark, "
                "schedule = EXCLUDED.schedule, definition = EXCLUDED.definition, "
                "run_id = EXCLUDED.run_id, recorded_at = EXCLUDED.recorded_at",
                [
                    copy.source,
                    copy.dataset,
                    copy.table,
                    copy.load_mode,
                    list(copy.primary_key),
                    copy.watermark,
                    copy.schedule,
                    Jsonb(copy.definition),
                    copy.run_id,
                    copy.recorded_at,
                ],
            )

    def skip_run(self, run: RunStart, *, ended_at: datetime) -> None:
        self._connection().execute(
            "INSERT INTO platform.pipeline_runs "
            "(run_id, source, dataset, trigger, status, started_at, ended_at) "
            "VALUES (%s, %s, %s, %s, 'skipped', %s, %s)",
            [run.run_id, run.source, run.dataset, run.trigger, run.started_at, ended_at],
        )

    def fail_interrupted_runs(
        self, source: str, dataset: str, *, found_by: UUID, ended_at: datetime
    ) -> int:
        updated = self._connection().execute(
            "UPDATE platform.pipeline_runs SET status = 'failed', ended_at = %s, "
            "error_class = %s, error_message = %s "
            "WHERE source = %s AND dataset = %s AND status = 'running'",
            [ended_at, INTERRUPTED, interrupted_message(found_by), source, dataset],
        )
        return updated.rowcount

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

    @contextmanager
    def stages(self) -> Iterator[PostgresStages]:
        """The stage tables and the V2 records; everything done through them commits together."""
        conn = self._connection()
        with conn.transaction():
            yield PostgresStages(conn)

    def fail_run(
        self,
        run: RunStart,
        *,
        ended_at: datetime,
        rows_extracted: int | None,
        failure: RunFailure,
    ) -> None:
        # Written rather than updated: a run that failed before `start_run` ever ran has no row
        # yet, and a failure nobody can see is the worst kind.
        updated = self._connection().execute(
            "INSERT INTO platform.pipeline_runs (run_id, source, dataset, trigger, status, "
            "started_at, ended_at, rows_extracted, error_class, error_message, error_traceback) "
            "VALUES (%s, %s, %s, %s, 'failed', %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (run_id) DO UPDATE SET status = 'failed', ended_at = EXCLUDED.ended_at, "
            "rows_extracted = EXCLUDED.rows_extracted, error_class = EXCLUDED.error_class, "
            "error_message = EXCLUDED.error_message, error_traceback = EXCLUDED.error_traceback "
            "WHERE pipeline_runs.status = 'running'",
            [
                run.run_id,
                run.source,
                run.dataset,
                run.trigger,
                run.started_at,
                ended_at,
                rows_extracted,
                failure.error_class,
                failure.message,
                failure.traceback,
            ],
        )
        if updated.rowcount != 1:
            raise LoadError(f"run {run.run_id} has already ended")
