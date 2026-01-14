"""
Add WAL metrics table.

Revision ID: 003_add_wal_metrics
Revises: 002_add_wal_monitor
Create Date: 2026-01-14 14:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers
revision: str = "003_add_wal_metrics"
down_revision: Union[str, None] = "002_add_wal_monitor"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create WAL metrics table."""
    op.create_table(
        "wal_metrics",
        sa.Column(
            "id",
            sa.Integer(),
            autoincrement=True,
            nullable=False,
            comment="Unique metric identifier",
        ),
        sa.Column(
            "source_id",
            sa.Integer(),
            nullable=False,
            comment="Reference to source being monitored",
        ),
        sa.Column(
            "size_bytes",
            sa.BigInteger(),
            nullable=False,
            comment="WAL size in bytes",
        ),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
            comment="When this metric was recorded",
        ),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        comment="PostgreSQL WAL size metrics for monitoring",
    )

    # Create indexes
    op.create_index(
        "idx_wal_metrics_source_id", "wal_metrics", ["source_id"], unique=False
    )
    op.create_index(
        "idx_wal_metrics_recorded_at", "wal_metrics", ["recorded_at"], unique=False
    )


def downgrade() -> None:
    """Drop WAL metrics table."""
    op.drop_index("idx_wal_metrics_recorded_at", table_name="wal_metrics")
    op.drop_index("idx_wal_metrics_source_id", table_name="wal_metrics")
    op.drop_table("wal_metrics")
