"""Saved incremental state per dataset, and the history of each dataset's columns.

Revision ID: 0002
Revises: 0001
"""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE platform.source_state (
            source text NOT NULL,
            dataset text NOT NULL,
            load_mode text NOT NULL CHECK (load_mode IN ('full', 'append', 'merge')),
            primary_key text[] NOT NULL DEFAULT '{}',
            watermark_column text,
            watermark_type text,
            watermark text,
            file_path text,
            file_sha256 text,
            config_sha256 text NOT NULL,
            run_id uuid NOT NULL,
            saved_at timestamptz NOT NULL,
            PRIMARY KEY (source, dataset)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE platform.schema_versions (
            source text NOT NULL,
            dataset text NOT NULL,
            version integer NOT NULL,
            columns jsonb NOT NULL,
            run_id uuid NOT NULL,
            recorded_at timestamptz NOT NULL,
            PRIMARY KEY (source, dataset, version)
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE platform.schema_versions")
    op.execute("DROP TABLE platform.source_state")
