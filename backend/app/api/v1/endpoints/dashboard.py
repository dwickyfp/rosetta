from typing import Any, Dict
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.domain.services.dashboard import DashboardService

router = APIRouter()

@router.get("/summary", response_model=Dict[str, Any])
def get_dashboard_summary(
    db: Session = Depends(get_db),
):
    """
    Get dashboard summary metrics.
    """
    service = DashboardService(db)
    
    pipeline_status = service.get_pipeline_status_summary()
    flow_stats = service.get_global_data_flow_stats(days=7) # Get short term for summary cards if needed, or just today/yesterday
    credit_stats = service.get_total_credit_usage(days=30)
    
    return {
        "pipelines": pipeline_status,
        "data_flow": {
            "today": flow_stats["total_today"],
            "yesterday": flow_stats["total_yesterday"]
        },
        "credits": {
            "month_total": credit_stats["current_month_total"]
        }
    }

@router.get("/flow-chart", response_model=Dict[str, Any])
def get_flow_chart_data(
    days: int = 7,
    db: Session = Depends(get_db),
):
    """
    Get data flow chart data.
    """
    service = DashboardService(db)
    return service.get_global_data_flow_stats(days=days)

@router.get("/credit-chart", response_model=Dict[str, Any])
def get_credit_chart_data(
    days: int = 30,
    db: Session = Depends(get_db),
):
    """
    Get credit usage chart data.
    """
    service = DashboardService(db)
    return service.get_total_credit_usage(days=days)
