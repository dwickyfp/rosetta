"""
Table Metadata model.

This module defines the TableMetadata model representing the list of tables being monitored.
"""

from datetime import datetime
from typing import Any, List, Optional

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.domain.models.base import Base


class TableMetadata(Base):
    """
    TableMetadata model.
    """

    __tablename__ = "table_metadata_list"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    source_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("sources.id", ondelete="CASCADE"), nullable=False
    )
    table_name: Mapped[str] = mapped_column(String(255), nullable=True)
    schema_table: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    
    is_exists_table_landing: Mapped[bool] = mapped_column(
        Boolean, default=False, comment="Table exists in landing (Snowflake)"
    )
    is_exists_task: Mapped[bool] = mapped_column(
        Boolean, default=False, comment="Task exists in Snowflake"
    )
    is_exists_table_destination: Mapped[bool] = mapped_column(
        Boolean, default=False, comment="Table exists in destination (Snowflake)"
    )
    is_changes_schema: Mapped[bool] = mapped_column(
        Boolean, default=False, comment="Whether schema changes were detected"
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )

    # Relationships
    source: Mapped["Source"] = relationship("Source", back_populates="tables")
    history_schema: Mapped[List["HistorySchemaEvolution"]] = relationship(
        "HistorySchemaEvolution", back_populates="table_metadata", cascade="all, delete-orphan"
    )
