"""
Table Metadata Repository.

Handles database operations for TableMetadata and HistorySchemaEvolution.
"""

from typing import List

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.domain.models.history_schema_evolution import HistorySchemaEvolution
from app.domain.models.table_metadata import TableMetadata
from app.domain.repositories.base import BaseRepository


class TableMetadataRepository(BaseRepository[TableMetadata]):
    """
    Repository for TableMetadata entity.
    """

    def __init__(self, db: Session):
        """Initialize repository."""
        super().__init__(TableMetadata, db)

    def get_by_source_id(self, source_id: int) -> List[TableMetadata]:
        """
        Get all table metadata for a specific source.
        
        Args:
            source_id: Source identifier
            
        Returns:
            List of table metadata
        """
        return (
            self.db.query(TableMetadata)
            .filter(TableMetadata.source_id == source_id)
            .all()
        )

    def get_version_count(self, table_metadata_id: int) -> int:
        """
        Get the version count (number of history entries) for a table.
        
        Args:
            table_metadata_id: Table metadata identifier
            
        Returns:
            Count of history schema evolution records
        """
        count = (
            self.db.query(func.count(HistorySchemaEvolution.id))
            .filter(HistorySchemaEvolution.table_metadata_list_id == table_metadata_id)
            .scalar()
        )
        return count or 0

    def get_tables_with_version_count(self, source_id: int) -> List[tuple[TableMetadata, int]]:
        """
        Get tables with their version count (history count).
        
        Args:
            source_id: Source identifier
            
        Returns:
            List of tuples (TableMetadata, version_count)
        """
        # Outer join to ensure we get tables even with 0 history
        query = (
            self.db.query(
                TableMetadata,
                func.count(HistorySchemaEvolution.id).label("version_count")
            )
            .outerjoin(HistorySchemaEvolution, TableMetadata.id == HistorySchemaEvolution.table_metadata_list_id)
            .filter(TableMetadata.source_id == source_id)
            .group_by(TableMetadata.id)
        )
        
        return query.all()

    def delete_by_source_id(self, source_id: int) -> None:
        """
        Delete all table metadata for a source.
        
        Args:
            source_id: Source identifier
        """
        self.db.query(TableMetadata).filter(TableMetadata.source_id == source_id).delete()
        self.db.flush()

    def delete_table(self, source_id: int, table_name: str) -> None:
        """
        Delete metadata for a specific table.

        Args:
            source_id: Source identifier
            table_name: Table name
        """
        self.db.query(TableMetadata).filter(
            TableMetadata.source_id == source_id, 
            TableMetadata.table_name == table_name
        ).delete()
        self.db.flush()

    def update_status(self, table_id: int, **kwargs) -> TableMetadata:
        """
        Update status flags for a table metadata.
        """
        table = self.get_by_id(table_id)
        for key, value in kwargs.items():
            if hasattr(table, key):
                setattr(table, key, value)
        self.db.commit()
        self.db.refresh(table)
        return table
