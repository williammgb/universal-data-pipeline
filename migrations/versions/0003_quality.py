"""Quarantined rows, quality check results, and the quarantined count of each run.

Revision ID: 0003
Revises: 0002
"""

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE platform.quarantine (
            quarantine_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            run_id uuid NOT NULL REFERENCES platform.pipeline_runs (run_id),
            source text NOT NULL,
            dataset text NOT NULL,
            reason text NOT NULL,
            record jsonb NOT NULL,
            quarantined_at timestamptz NOT NULL
        )
        """
    )
    op.execute("CREATE INDEX quarantine_run ON platform.quarantine (run_id)")
    op.execute(
        """
        CREATE TABLE platform.quality_results (
            run_id uuid NOT NULL REFERENCES platform.pipeline_runs (run_id),
            source text NOT NULL,
            dataset text NOT NULL,
            position integer NOT NULL,
            check_type text NOT NULL,
            columns text[] NOT NULL DEFAULT '{}',
            severity text NOT NULL CHECK (severity IN ('warn', 'error')),
            passed boolean NOT NULL,
            failing_rows bigint,
            table_rows bigint,
            message text NOT NULL,
            settings jsonb NOT NULL,
            checked_at timestamptz NOT NULL,
            PRIMARY KEY (run_id, position)
        )
        """
    )
    op.execute("ALTER TABLE platform.pipeline_runs ADD COLUMN rows_quarantined bigint")


def downgrade() -> None:
    op.execute("ALTER TABLE platform.pipeline_runs DROP COLUMN rows_quarantined")
    op.execute("DROP TABLE platform.quality_results")
    op.execute("DROP TABLE platform.quarantine")
