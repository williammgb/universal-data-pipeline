"""Which ingest run's RAW a pipeline execution read: for a full load the run whose rows it read,
for an append or a merge the last run that had added to RAW when it read every row. Empty until
the execution has read its input, and for one that failed before it could.

Revision ID: 0008
Revises: 0007
"""

from alembic import op

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE platform.pipeline_executions "
        "ADD COLUMN input_run_id uuid REFERENCES platform.pipeline_runs (run_id)"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE platform.pipeline_executions DROP COLUMN input_run_id")
