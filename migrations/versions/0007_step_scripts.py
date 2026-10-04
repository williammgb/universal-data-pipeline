"""What a python step's script left in its step record: the hash of the code that ran, what it
printed, and the script line it failed on. All three stay empty for the other step types.

Revision ID: 0007
Revises: 0006
"""

from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE platform.step_executions
            ADD COLUMN script_sha256 text CHECK (script_sha256 ~ '^[0-9a-f]{64}$'),
            ADD COLUMN output text,
            ADD COLUMN error_line integer CHECK (error_line >= 1)
        """
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE platform.step_executions "
        "DROP COLUMN script_sha256, DROP COLUMN output, DROP COLUMN error_line"
    )
