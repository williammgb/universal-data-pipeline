"""The RAW, STAGING and CLEAN stages, and the tables V2's pipelines write into.

Each stage is a schema holding `<source>__<dataset>`; `docs/stages-and-pipelines.md` explains
them. RAW tables are append-only: `platform.refuse_raw_change` is attached to each one when it
is created and refuses UPDATE, DELETE and TRUNCATE.

`platform.pipeline_runs` stays what it was, the ingest runs. A run of a V2 pipeline is a row in
`platform.pipeline_executions`; profiles, constraint results and lineage each name exactly one
of the two.

Revision ID: 0006
Revises: 0005
"""

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None

# Which run produced a row: an ingest run or a pipeline execution, never both, never neither.
_PRODUCED_BY = """
    ingest_run_id uuid REFERENCES platform.pipeline_runs (run_id),
    execution_id uuid REFERENCES platform.pipeline_executions (execution_id),
    CHECK (num_nonnulls(ingest_run_id, execution_id) = 1)
"""

_STAGE = "stage text NOT NULL CHECK (stage IN ('raw', 'staging', 'clean'))"


def upgrade() -> None:
    for schema in ("raw", "staging", "clean"):
        op.execute(f"CREATE SCHEMA {schema}")
    op.execute(
        """
        CREATE FUNCTION platform.refuse_raw_change() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'raw.% is append-only: % is not allowed', TG_TABLE_NAME, TG_OP
                USING ERRCODE = 'restrict_violation';
        END
        $$
        """
    )
    op.execute(
        """
        CREATE TABLE platform.pipelines (
            pipeline_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            source text NOT NULL,
            dataset text NOT NULL,
            name text NOT NULL,
            created_at timestamptz NOT NULL,
            UNIQUE (source, dataset, name)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE platform.pipeline_versions (
            pipeline_id bigint NOT NULL REFERENCES platform.pipelines (pipeline_id),
            version integer NOT NULL CHECK (version >= 1),
            definition jsonb NOT NULL,
            created_at timestamptz NOT NULL,
            PRIMARY KEY (pipeline_id, version)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE platform.pipeline_steps (
            pipeline_id bigint NOT NULL,
            version integer NOT NULL,
            position integer NOT NULL CHECK (position >= 1),
            step_type text NOT NULL CHECK (step_type <> ''),
            configuration jsonb NOT NULL,
            PRIMARY KEY (pipeline_id, version, position),
            FOREIGN KEY (pipeline_id, version) REFERENCES platform.pipeline_versions
        )
        """
    )
    op.execute(
        """
        CREATE TABLE platform.pipeline_executions (
            execution_id uuid PRIMARY KEY,
            pipeline_id bigint NOT NULL,
            version integer NOT NULL,
            trigger text NOT NULL CHECK (trigger IN ('manual', 'scheduled')),
            status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
            started_at timestamptz NOT NULL,
            ended_at timestamptz,
            rows_in bigint,
            rows_out bigint,
            failed_step integer,
            error_class text,
            error_message text,
            error_traceback text,
            FOREIGN KEY (pipeline_id, version) REFERENCES platform.pipeline_versions,
            CHECK ((status = 'running') = (ended_at IS NULL))
        )
        """
    )
    op.execute(
        "CREATE INDEX pipeline_executions_newest "
        "ON platform.pipeline_executions (pipeline_id, started_at DESC)"
    )
    op.execute(
        """
        CREATE TABLE platform.step_executions (
            execution_id uuid NOT NULL REFERENCES platform.pipeline_executions (execution_id),
            position integer NOT NULL CHECK (position >= 1),
            status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
            started_at timestamptz NOT NULL,
            ended_at timestamptz,
            rows_in bigint,
            rows_out bigint,
            values_changed bigint,
            error_class text,
            error_message text,
            PRIMARY KEY (execution_id, position),
            CHECK ((status = 'running') = (ended_at IS NULL))
        )
        """
    )
    op.execute(
        f"""
        CREATE TABLE platform.profiles (
            profile_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            source text NOT NULL,
            dataset text NOT NULL,
            {_STAGE},
            after_step integer CHECK (after_step >= 1),
            {_PRODUCED_BY},
            table_rows bigint NOT NULL CHECK (table_rows >= 0),
            result jsonb NOT NULL,
            profiled_at timestamptz NOT NULL,
            CHECK (after_step IS NULL OR (stage = 'staging' AND execution_id IS NOT NULL))
        )
        """
    )
    op.execute(
        "CREATE INDEX profiles_by_stage ON platform.profiles (source, dataset, stage, profiled_at)"
    )
    op.execute(
        f"""
        CREATE TABLE platform.constraint_results (
            result_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            source text NOT NULL,
            dataset text NOT NULL,
            {_STAGE},
            after_step integer CHECK (after_step >= 1),
            {_PRODUCED_BY},
            position integer NOT NULL CHECK (position >= 1),
            constraint_type text NOT NULL,
            columns text[] NOT NULL,
            critical boolean NOT NULL,
            passed boolean NOT NULL,
            failing_rows bigint CHECK (failing_rows >= 0),
            failing_values bigint CHECK (failing_values >= 0),
            message text NOT NULL,
            settings jsonb NOT NULL,
            checked_at timestamptz NOT NULL,
            CHECK (after_step IS NULL OR (stage = 'staging' AND execution_id IS NOT NULL))
        )
        """
    )
    op.execute(
        "CREATE INDEX constraint_results_by_stage "
        "ON platform.constraint_results (source, dataset, stage, checked_at)"
    )
    op.execute(
        """
        CREATE TABLE platform.constraint_violations (
            violation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            result_id bigint NOT NULL REFERENCES platform.constraint_results (result_id),
            column_name text NOT NULL,
            row_key jsonb NOT NULL,
            value text
        )
        """
    )
    op.execute(
        "CREATE INDEX constraint_violations_result ON platform.constraint_violations (result_id)"
    )
    op.execute(
        f"""
        CREATE TABLE platform.lineage (
            lineage_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            source text NOT NULL,
            dataset text NOT NULL,
            {_PRODUCED_BY},
            position integer NOT NULL CHECK (position >= 1),
            node text NOT NULL CHECK (node IN ('source', 'raw', 'step', 'clean')),
            name text NOT NULL,
            step_position integer CHECK (step_position >= 1),
            recorded_at timestamptz NOT NULL,
            CHECK ((node = 'step') = (step_position IS NOT NULL)),
            UNIQUE (ingest_run_id, position),
            UNIQUE (execution_id, position)
        )
        """
    )
    op.execute("CREATE INDEX lineage_by_dataset ON platform.lineage (source, dataset)")


def downgrade() -> None:
    for table in (
        "lineage",
        "constraint_violations",
        "constraint_results",
        "profiles",
        "step_executions",
        "pipeline_executions",
        "pipeline_steps",
        "pipeline_versions",
        "pipelines",
    ):
        op.execute(f"DROP TABLE platform.{table}")
    for schema in ("clean", "staging", "raw"):
        op.execute(f"DROP SCHEMA {schema} CASCADE")
    op.execute("DROP FUNCTION platform.refuse_raw_change()")
