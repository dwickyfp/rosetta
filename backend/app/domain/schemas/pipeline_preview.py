from typing import Any, List, Dict, Optional
from pydantic import BaseModel

class PipelinePreviewRequest(BaseModel):
    """Request model for previewing custom SQL."""
    sql: str
    destination_id: int
    table_name: str
    source_id: int

class PipelinePreviewResponse(BaseModel):
    """Response model for previewing custom SQL."""
    columns: List[str]
    column_types: List[str]
    data: List[Dict[str, Any]]
    error: Optional[str] = None
