"""
Add landing database and schema to destination.

Revision ID: 004_add_landing_config
Revises: 003_add_wal_metrics
Create Date: 2026-01-19 17:30:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "004_add_landing_config"
down_revision: Union[str, None] = "003_add_wal_metrics"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add landing configuration columns to destinations table."""
    op.add_column(
        "destinations",
        sa.Column(
            "snowflake_landing_database",
            sa.String(length=255),
            nullable=True,
            comment="Snowflake landing database name",
        ),
    )
    op.add_column(
        "destinations",
        sa.Column(
            "snowflake_landing_schema",
            sa.String(length=255),
            nullable=True,
            comment="Snowflake landing schema name",
        ),
    )


def downgrade() -> None:
    """Remove landing configuration columns from destinations table."""
    op.drop_column("destinations", "snowflake_landing_schema")
    op.drop_column("destinations", "snowflake_landing_database")
