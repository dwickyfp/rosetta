"""
Dashboard Service.

Aggregates data for the main dashboard view.
"""

from typing import Dict, List, Any
from sqlalchemy.orm import Session
from sqlalchemy import select, func, desc, case
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from app.core.database import db_manager
from app.core.logging import get_logger
from app.domain.models.pipeline import Pipeline, PipelineStatus
from app.domain.models.data_flow_monitoring import DataFlowRecordMonitoring
from app.domain.models.credit_snowflake_monitoring import CreditSnowflakeMonitoring

logger = get_logger(__name__)

class DashboardService:
    """
    Service for aggregating dashboard data.
    """

    def __init__(self, db: Session):
        self.db = db

    def get_pipeline_status_summary(self) -> Dict[str, int]:
        """
        Get count of pipelines by status.
        """
        # Group by status and count
        results = self.db.execute(
            select(
                Pipeline.status,
                func.count(Pipeline.id)
            ).group_by(Pipeline.status)
        ).all()
        
        # Initialize with defaults (ensure all statuses are present if 0)
        summary = {
            "START": 0,
            "PAUSE": 0,
            # Add other known statuses if any, or mapped from generic status
        }
        
        total = 0
        for status, count in results:
            summary[status] = count
            total += count
            
        summary["total"] = total
        return summary

    def get_global_data_flow_stats(self, days: int = 7) -> Dict[str, Any]:
        """
        Get aggregated data flow stats, grouped by Pipeline.
        Returns:
            - total_today: int
            - total_yesterday: int
            - history: List[Dict] -> [{"date": "2023-01-01", "Pipeline A": 100, "Pipeline B": 20}]
            - pipelines: List[str] -> ["Pipeline A", "Pipeline B"]
        """
        today = datetime.now(ZoneInfo('Asia/Jakarta')).date()
        yesterday = today - timedelta(days=1)
        start_date = today - timedelta(days=days)
        
        # 1. Totals (Global)
        total_today = self.db.execute(
            select(func.sum(DataFlowRecordMonitoring.record_count))
            .where(func.date(DataFlowRecordMonitoring.created_at) == today)
        ).scalar() or 0
        
        total_yesterday = self.db.execute(
            select(func.sum(DataFlowRecordMonitoring.record_count))
            .where(func.date(DataFlowRecordMonitoring.created_at) == yesterday)
        ).scalar() or 0
        
        # 2. History Grouped by Pipeline
        # We need Pipeline Name. Join with Pipeline table.
        history_results = self.db.execute(
            select(
                func.date(DataFlowRecordMonitoring.created_at).label("day"),
                Pipeline.name,
                func.sum(DataFlowRecordMonitoring.record_count)
            )
            .join(Pipeline, Pipeline.id == DataFlowRecordMonitoring.pipeline_id)
            .where(DataFlowRecordMonitoring.created_at >= start_date)
            .group_by(func.date(DataFlowRecordMonitoring.created_at), Pipeline.name)
            .order_by("day")
        ).all()
        
        # Pivot data for Recharts
        # Structure: {"2023-01-01": {"date": "2023-01-01", "Pipeline A": 100}}
        pivot_data = {}
        pipeline_names = set()

        # Initialize all dates first like before? 
        # Better to fill gaps later or just let frontend handle it.
        # Let's initialize structure with 0s for known pipelines if possible, 
        # but we don't know all pipelines efficiently without another query.
        # Let's just pivot what we have and fill gaps.
        
        for row in history_results:
            d_str = row[0].isoformat()
            p_name = row[1]
            count = row[2]
            pipeline_names.add(p_name)
            
            if d_str not in pivot_data:
                pivot_data[d_str] = {"date": d_str}
            
            pivot_data[d_str][p_name] = count

        # Fill missing dates
        filled_history = []
        for i in range(days + 1):
            d = (start_date + timedelta(days=i))
            d_str = d.isoformat()
            
            entry = pivot_data.get(d_str, {"date": d_str})
            # Ensure all pipelines exist in entry with 0 if missing (good for stacked charts)
            for p_name in pipeline_names:
                if p_name not in entry:
                    entry[p_name] = 0
            
            filled_history.append(entry)
            
        return {
            "total_today": total_today,
            "total_yesterday": total_yesterday,
            "history": filled_history,
            "pipelines": list(pipeline_names)
        }

    def get_total_credit_usage(self, days: int = 30) -> Dict[str, Any]:
        """
        Get aggregated credit usage, grouped by Destination.
        Returns:
            - current_month_total: float
            - history: List[Dict] -> [{"date": "...", "Dest A": 1.5, "Dest B": 0.5}]
            - destinations: List[str]
        """
        now = datetime.now(ZoneInfo('Asia/Jakarta'))
        start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        start_date = now - timedelta(days=days)
        
        # 1. Total (Global Month)
        month_total = self.db.execute(
            select(func.sum(CreditSnowflakeMonitoring.total_credit))
            .where(CreditSnowflakeMonitoring.usage_date >= start_of_month)
        ).scalar() or 0.0
        
        # 2. History Grouped by Destination
        # Need Destination name. Join.
        from app.domain.models.destination import Destination
        
        history_results = self.db.execute(
            select(
                CreditSnowflakeMonitoring.usage_date,
                Destination.name,
                func.sum(CreditSnowflakeMonitoring.total_credit)
            )
            .join(Destination, Destination.id == CreditSnowflakeMonitoring.destination_id)
            .where(CreditSnowflakeMonitoring.usage_date >= start_date)
            .group_by(CreditSnowflakeMonitoring.usage_date, Destination.name)
            .order_by(CreditSnowflakeMonitoring.usage_date)
        ).all()
        
        pivot_data = {}
        dest_names = set()
        
        for row in history_results:
            d_str = row[0].date().isoformat() # usage_date is datetime, want YYYY-MM-DD
            d_name = row[1]
            credits = float(row[2])
            dest_names.add(d_name)
            
            if d_str not in pivot_data:
                pivot_data[d_str] = {"date": d_str}
            
            pivot_data[d_str][d_name] = credits

        # Fill missing dates
        filled_history = []
        for i in range(days + 1):
            d = (start_date + timedelta(days=i))
            d_str = d.date().isoformat()
            
            entry = pivot_data.get(d_str, {"date": d_str})
            for name in dest_names:
                if name not in entry:
                    entry[name] = 0.0
            
            filled_history.append(entry)

        return {
            "current_month_total": month_total,
            "history": filled_history,
            "destinations": list(dest_names)
        }
