"""Each source's and dataset's settings as written, copied by every run, and runs by dataset.

Revision ID: 0004
Revises: 0003
"""

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE platform.sources (
            source text PRIMARY KEY,
            connector_type text NOT NULL,
            connection jsonb NOT NULL,
            run_id uuid NOT NULL REFERENCES platform.pipeline_runs (run_id),
            recorded_at timestamptz NOT NULL
        )
        """
    )
    op.execute(
        """
        CREATE TABLE platform.datasets (
            source text NOT NULL REFERENCES platform.sources (source),
            dataset text NOT NULL,
            table_name text NOT NULL,
            load_mode text NOT NULL,
            primary_key text[] NOT NULL DEFAULT '{}',
            watermark text,
            schedule text,
            definition jsonb NOT NULL,
            run_id uuid NOT NULL REFERENCES platform.pipeline_runs (run_id),
            recorded_at timestamptz NOT NULL,
            PRIMARY KEY (source, dataset)
        )
        """
    )
    op.execute(
        "CREATE INDEX pipeline_runs_dataset "
        "ON platform.pipeline_runs (source, dataset, started_at DESC)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX platform.pipeline_runs_dataset")
    op.execute("DROP TABLE platform.datasets")
    op.execute("DROP TABLE platform.sources")
