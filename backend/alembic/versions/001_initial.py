"""
Initial migration - Create all tables.

Revision ID: 001_initial
Create Date: 2024-01-14 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision: str = "001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create initial database schema."""
    # Execute the existing SQL migration
    # This assumes the SQL files in migrations/ have been executed
    pass


def downgrade() -> None:
    """Drop all tables."""
    op.drop_table("wal_metrics")
    op.drop_table("pipeline_metadata")
    op.drop_table("pipelines")
    op.drop_table("destinations")
    op.drop_table("sources")
