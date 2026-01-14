"""
Pipeline models - ETL pipeline configurations and metadata.

Represents the mapping between sources and destinations with runtime metadata.
"""

from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.domain.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.domain.models.destination import Destination
    from app.domain.models.source import Source


class PipelineStatus(str, Enum):
    """Pipeline operational status."""

    START = "START"
    PAUSE = "PAUSE"
    REFRESH = "REFRESH"


class PipelineMetadataStatus(str, Enum):
    """Pipeline runtime status."""

    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    ERROR = "ERROR"


class Pipeline(Base, TimestampMixin):
    """
    ETL Pipeline configuration.

    Connects a data source to a destination and manages the ETL flow.
    """

    __tablename__ = "pipelines"
    __table_args__ = (
        UniqueConstraint("name", name="uq_pipelines_name"),
        {"comment": "ETL pipeline configurations"},
    )

    # Primary Key
    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique pipeline identifier",
    )

    # Pipeline Identification
    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        unique=True,
        index=True,
        comment="Unique pipeline name",
    )

    # Source and Destination References
    source_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("sources.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Reference to source configuration",
    )

    destination_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("destinations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Reference to destination configuration",
    )

    # Pipeline Status
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=PipelineStatus.START.value,
        index=True,
        comment="Pipeline operational status: START, PAUSE, or REFRESH",
    )

    # Relationships
    source: Mapped["Source"] = relationship(
        "Source", back_populates="pipelines", lazy="selectin"
    )

    destination: Mapped["Destination"] = relationship(
        "Destination", back_populates="pipelines", lazy="selectin"
    )

    pipeline_metadata: Mapped["PipelineMetadata"] = relationship(
        "PipelineMetadata",
        back_populates="pipeline",
        uselist=False,
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        """String representation."""
        return f"Pipeline(id={self.id}, name={self.name!r}, " f"status={self.status!r})"

    @property
    def is_active(self) -> bool:
        """Check if pipeline is in START status."""
        return self.status == PipelineStatus.START.value

    @property
    def is_paused(self) -> bool:
        """Check if pipeline is in PAUSE status."""
        return self.status == PipelineStatus.PAUSE.value

    def start(self) -> None:
        """Set pipeline status to START."""
        self.status = PipelineStatus.START.value

    def pause(self) -> None:
        """Set pipeline status to PAUSE."""
        self.status = PipelineStatus.PAUSE.value

    def refresh(self) -> None:
        """Set pipeline status to REFRESH."""
        self.status = PipelineStatus.REFRESH.value


class PipelineMetadata(Base, TimestampMixin):
    """
    Pipeline runtime metadata.

    Tracks the runtime state and error information for pipelines.
    """

    __tablename__ = "pipeline_metadata"
    __table_args__ = {"comment": "Pipeline runtime metadata and error tracking"}

    # Primary Key
    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique metadata identifier",
    )

    # Pipeline Reference
    pipeline_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("pipelines.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
        comment="Reference to pipeline",
    )

    # Runtime Status
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=PipelineMetadataStatus.RUNNING.value,
        index=True,
        comment="Runtime status: RUNNING, PAUSED, or ERROR",
    )

    # Error Tracking
    last_error: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="Last error message"
    )

    last_error_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="Timestamp of last error"
    )

    # Execution Tracking
    last_start_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp of last pipeline start",
    )

    # Relationships
    pipeline: Mapped["Pipeline"] = relationship(
        "Pipeline", back_populates="pipeline_metadata"
    )

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"PipelineMetadata(id={self.id}, pipeline_id={self.pipeline_id}, "
            f"status={self.status!r})"
        )

    def set_running(self) -> None:
        """Set status to RUNNING."""
        self.status = PipelineMetadataStatus.RUNNING.value
        self.last_start_at = datetime.utcnow()

    def set_paused(self) -> None:
        """Set status to PAUSED."""
        self.status = PipelineMetadataStatus.PAUSED.value

    def set_error(self, error_message: str) -> None:
        """
        Set status to ERROR and record error details.

        Args:
            error_message: Description of the error
        """
        self.status = PipelineMetadataStatus.ERROR.value
        self.last_error = error_message
        self.last_error_at = datetime.utcnow()

    def clear_error(self) -> None:
        """Clear error state and set to RUNNING."""
        self.status = PipelineMetadataStatus.RUNNING.value
        self.last_error = None
        self.last_error_at = None
