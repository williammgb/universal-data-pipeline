"""Edits made to a dataset's configuration from the dashboard, kept as an append-only history.

The newest row of a dataset is its current override; `override` holds the complete override
after that edit, so reverting a field leaves a row whose override no longer names it.

Revision ID: 0005
Revises: 0004
"""

from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE platform.config_edits (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            source text NOT NULL,
            dataset text NOT NULL,
            override jsonb NOT NULL,
            changed jsonb NOT NULL,
            changed_at timestamptz NOT NULL
        )
        """
    )
    op.execute(
        "CREATE INDEX config_edits_newest ON platform.config_edits (source, dataset, id DESC)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX platform.config_edits_newest")
    op.execute("DROP TABLE platform.config_edits")
