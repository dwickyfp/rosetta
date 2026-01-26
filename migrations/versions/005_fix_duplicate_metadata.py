"""add unique constraint table metadata

Revision ID: 005_fix_duplicate_metadata
Revises: 004_snowflake_ddl
Create Date: 2024-01-26 13:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '005_fix_duplicate_metadata'
down_revision = '004_snowflake_ddl'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Deduplicate by keeping the one with the Latest ID (or any logic)
    # We want to keep one entry per (source_id, table_name).
    # Using a DELETE with a subquery is standard for postgres.
    
    op.execute("""
        DELETE FROM table_metadata_list a USING (
            SELECT MIN(id) as min_id, source_id, table_name
            FROM table_metadata_list
            GROUP BY source_id, table_name
            HAVING COUNT(*) > 1
        ) b
        WHERE a.source_id = b.source_id 
        AND a.table_name = b.table_name 
        AND a.id <> b.min_id;
    """)

    # 2. Add Unique Constraint
    op.create_unique_constraint(
        'uq_table_metadata_source_table',
        'table_metadata_list',
        ['source_id', 'table_name']
    )


def downgrade() -> None:
    op.drop_constraint('uq_table_metadata_source_table', 'table_metadata_list', type_='unique')
