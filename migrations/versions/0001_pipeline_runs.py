"""Platform and datasets schemas, and the pipeline run history.

Revision ID: 0001
Revises:
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA platform")
    op.execute("CREATE SCHEMA datasets")
    op.execute(
        """
        CREATE TABLE platform.pipeline_runs (
            run_id uuid PRIMARY KEY,
            source text NOT NULL,
            dataset text NOT NULL,
            trigger text NOT NULL CHECK (trigger IN ('manual', 'scheduled')),
            status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'skipped')),
            started_at timestamptz NOT NULL,
            ended_at timestamptz,
            rows_extracted bigint,
            rows_loaded bigint,
            error_class text,
            error_message text,
            error_traceback text,
            CHECK ((status = 'running') = (ended_at IS NULL))
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE platform.pipeline_runs")
    op.execute("DROP SCHEMA datasets CASCADE")
    op.execute("DROP SCHEMA platform")
