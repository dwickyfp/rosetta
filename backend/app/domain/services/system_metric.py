from typing import List, Optional
from sqlalchemy.orm import Session
from app.domain.repositories.system_metric import SystemMetricRepository
from app.domain.schemas.system_metric import SystemMetricCreate
from app.domain.models.system_metric import SystemMetric

class SystemMetricService:
    def __init__(self, db: Session):
        self.repository = SystemMetricRepository(db)

    def get_latest_metrics(self) -> Optional[SystemMetric]:
        return self.repository.get_latest()

    def get_metrics_history(self, limit: int = 100) -> List[SystemMetric]:
        return self.repository.get_history(limit)
