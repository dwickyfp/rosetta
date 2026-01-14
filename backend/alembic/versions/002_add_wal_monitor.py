"""
Add WAL monitor table with upsert support.

Revision ID: 002_add_wal_monitor
Revises: 001_initial
Create Date: 2026-01-14 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision: str = "002_add_wal_monitor"
down_revision: Union[str, None] = "001_initial"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create WAL monitor table with upsert constraint."""
    # Create wal_monitor table
    op.create_table(
        "wal_monitor",
        sa.Column(
            "id",
            sa.Integer(),
            autoincrement=True,
            nullable=False,
            comment="Unique monitor record identifier",
        ),
        sa.Column(
            "source_id",
            sa.Integer(),
            nullable=False,
            comment="Reference to source being monitored",
        ),
        sa.Column(
            "wal_lsn",
            sa.String(length=255),
            nullable=True,
            comment="Log Sequence Number (e.g., '0/1A2B3C4D')",
        ),
        sa.Column(
            "wal_position",
            sa.BigInteger(),
            nullable=True,
            comment="WAL position as numeric value",
        ),
        sa.Column(
            "last_wal_received",
            sa.DateTime(),
            nullable=True,
            comment="Last time WAL data was received",
        ),
        sa.Column(
            "last_transaction_time",
            sa.DateTime(),
            nullable=True,
            comment="Last transaction timestamp",
        ),
        sa.Column(
            "replication_slot_name",
            sa.String(length=255),
            nullable=True,
            comment="Name of the replication slot",
        ),
        sa.Column(
            "replication_lag_bytes",
            sa.BigInteger(),
            nullable=True,
            comment="Replication lag in bytes",
        ),
        sa.Column(
            "status",
            sa.String(length=20),
            nullable=False,
            server_default="ACTIVE",
            comment="Monitor status: ACTIVE, IDLE, ERROR",
        ),
        sa.Column(
            "error_message", sa.Text(), nullable=True, comment="Error details if any"
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("NOW()"),
            comment="Record creation timestamp",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("NOW()"),
            comment="Last update timestamp",
        ),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_id", name="unique_source_wal"),
        comment="Real-time WAL replication status per source",
    )

    # Create indexes
    op.create_index(
        "idx_wal_monitor_source_id", "wal_monitor", ["source_id"], unique=False
    )
    op.create_index("idx_wal_monitor_status", "wal_monitor", ["status"], unique=False)
    op.create_index(
        "idx_wal_monitor_last_received",
        "wal_monitor",
        ["last_wal_received"],
        unique=False,
    )


def downgrade() -> None:
    """Drop WAL monitor table."""
    op.drop_index("idx_wal_monitor_last_received", table_name="wal_monitor")
    op.drop_index("idx_wal_monitor_status", table_name="wal_monitor")
    op.drop_index("idx_wal_monitor_source_id", table_name="wal_monitor")
    op.drop_table("wal_monitor")
