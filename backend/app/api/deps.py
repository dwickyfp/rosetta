"""
API dependencies for dependency injection.

Provides common dependencies used across API endpoints.
"""

from typing import Generator

from fastapi import Depends
from sqlalchemy.orm import Session

from app.core.database import get_db_session
from app.domain.services.destination import DestinationService
from app.domain.services.pipeline import PipelineService
from app.domain.services.source import SourceService


def get_db() -> Generator[Session, None, None]:
    """
    Get database session dependency.

    Yields database session for use in endpoint functions.
    """
    yield from get_db_session()


def get_source_service(db: Session = Depends(get_db)) -> SourceService:
    """
    Get source service dependency.

    Args:
        db: Database session

    Returns:
        Source service instance
    """
    return SourceService(db)


def get_destination_service(db: Session = Depends(get_db)) -> DestinationService:
    """
    Get destination service dependency.

    Args:
        db: Database session

    Returns:
        Destination service instance
    """
    return DestinationService(db)


def get_pipeline_service(db: Session = Depends(get_db)) -> PipelineService:
    """
    Get pipeline service dependency.

    Args:
        db: Database session

    Returns:
        Pipeline service instance
    """
    return PipelineService(db)
