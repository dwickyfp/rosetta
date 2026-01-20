from datetime import datetime
from sqlalchemy import Column, Integer, String, BigInteger, DateTime, ForeignKey
from sqlalchemy.orm import relationship

from app.domain.models.base import Base

class DataFlowRecordMonitoring(Base):
    __tablename__ = "data_flow_record_monitoring"

    id = Column(Integer, primary_key=True, index=True)
    pipeline_id = Column(Integer, ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False)
    source_id = Column(Integer, ForeignKey("sources.id", ondelete="CASCADE"), nullable=False)
    table_name = Column(String(255), nullable=False)
    record_count = Column(BigInteger, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    pipeline = relationship("Pipeline", back_populates="data_flow_records")
    source = relationship("Source", back_populates="data_flow_records")
