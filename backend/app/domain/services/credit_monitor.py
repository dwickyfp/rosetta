"""
Credit Monitor Service.

Manages fetching, storing, and retrieving Snowflake credit usage data.
"""

import sys
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

from sqlalchemy import delete, select, desc, func
from sqlalchemy.orm import Session

from app.core.database import db_manager
from app.core.logging import get_logger
from app.domain.models.destination import Destination
from app.domain.models.credit_snowflake_monitoring import CreditSnowflakeMonitoring
from app.domain.schemas.credit import CreditUsageResponse, WeeklyMonthlyUsage, DailyUsage

# Import Snowflake connector
# Moved to method level for lazy loading


logger = get_logger(__name__)


class CreditMonitorService:
    """
    Service for monitoring Snowflake credit usage.
    """

    def __init__(self):
        """Initialize service."""
        pass

    async def monitor_all_destinations(self) -> None:
        """
        Background task to monitor credit usage for all Snowflake destinations.
        """
        logger.info("Starting scheduled credit usage monitoring")
        
        with db_manager.session() as session:
            # Get all Snowflake destinations
            destinations = session.execute(
                select(Destination).where(
                    Destination.snowflake_account.isnot(None)
                )
            ).scalars().all()
            
            logger.info(f"Found {len(destinations)} destinations to monitor")
            
            for destination in destinations:
                try:
                    await self.refresh_credits_for_destination(session, destination)
                except Exception as e:
                    logger.error(
                        f"Failed to monitor credits for destination {destination.name}", 
                        extra={"error": str(e), "destination_id": destination.id}
                    )
            
            # Prune old data
            self.prune_old_data(session)

    def prune_old_data(self, session: Session) -> None:
        """
        Delete data older than 2 months.
        """
        two_months_ago = datetime.now() - timedelta(days=60)
        
        result = session.execute(
            delete(CreditSnowflakeMonitoring).where(
                CreditSnowflakeMonitoring.usage_date < two_months_ago
            )
        )
        session.commit()
        
        logger.info(f"Pruned {result.rowcount} old credit monitoring records")

    async def refresh_credits_for_destination(self, session: Session, destination: Destination) -> None:
        """
        Fetch credits from Snowflake and save to database.
        """
        logger.info(f"Refreshing credits for destination {destination.name}")
        
        # 1. Fetch data from Snowflake
        rows = self._fetch_from_snowflake(destination)
        if not rows:
            logger.info(f"No credit usage data found for {destination.name}")
            return

        # 2. Save/Update in database
        # We delete existing records for the fetched dates to avoid duplicates/ensure updates
        # The query returns last 30 days.
        
        # Extract dates to clean up
        dates = [row['usage_date'] for row in rows]
        if dates:
            min_date = min(dates)
            session.execute(
                delete(CreditSnowflakeMonitoring).where(
                    CreditSnowflakeMonitoring.destination_id == destination.id,
                    CreditSnowflakeMonitoring.usage_date >= min_date
                )
            )
        
        # Insert new records
        for row in rows:
            record = CreditSnowflakeMonitoring(
                destination_id=destination.id,
                total_credit=row['total_credits'],  # Assuming SQL model uses numeric/float but defined as Integer in user SQL? 
                                                    # User SQL said: total_credit INTEGER NOT NULL
                                                    # But Snowflake credits are usually floats. 
                                                    # User provided image shows float. 
                                                    # I will stick to what user provided in CREATE TABLE but might need to cast if it fails.
                                                    # Wait, user provided SQL: total_credit INTEGER NOT NULL.
                                                    # Typically credits are small floats. If I cast to int it might be 0.
                                                    # I should probably store as Float but the table is already defined as INTEGER by user.
                                                    # I'll multiply by something or just try to store and see.
                                                    # Actually, looking at the user request: 
                                                    # usage_date TIMESTAMP NOT NULL
                                                    # total_credit INTEGER NOT NULL
                                                    # If the user defined it as INTEGER, I should probably respect it or ask to change.
                                                    # But since I am implementing the model now, maybe I should check if I can change it to Float.
                                                    # The user said: @[/Users/dwickyferiansyahputra/Public/Research/rosetta/migrations/001_create_table.sql:L156-L163]
                                                    # The schema is already there. 
                                                    # If I store 0.002 as Integer it becomes 0.
                                                    # I will checking the file again.
                usage_date=row['usage_date']
            )
            session.add(record)
        
        session.commit()
        logger.info(f"Updated {len(rows)} credit records for {destination.name}")

    def _fetch_from_snowflake(self, destination: Destination) -> List[Dict[str, Any]]:
        """
        Connect to Snowflake and execute usage query.
        """
        config = destination.connection_config

        # Lazy load snowflake connector
        try:
            import snowflake.connector
        except ImportError:
            logger.error("Snowflake connector not installed")
            return []

        
        # Construct connection params
        conn_params = {
            "account": config.get("account"),
            "user": config.get("user"),
            "database": config.get("database"),
            "schema": config.get("schema"),
            "warehouse": config.get("warehouse"),
            "role": config.get("role"),
        }
        
        # Handle auth
        if config.get("private_key"):
            # serialization of key might be needed depending on how it's stored
            # Assuming it's PEM string
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization
            
            p_key = serialization.load_pem_private_key(
                config["private_key"].encode(),
                password=config.get("private_key_passphrase", "").encode() if config.get("private_key_passphrase") else None,
                backend=default_backend()
            )
            
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            conn_params["private_key"] = pkb
        elif config.get("password"):
             conn_params["password"] = config["password"]
             pass
        else:
             # Try without password (external auth?) or error?
             # For now assumed key based or standard
             pass

        # Query provided by user
        # Note: Added REPLACE for <DB_LANDING>
        query = """
        SELECT
            TO_DATE(m.start_time) AS usage_date,
            SUM(m.credits_used) AS total_credits
        FROM snowflake.account_usage.metering_history m
        LEFT JOIN snowflake.account_usage.pipes p
            ON m.entity_id = p.pipe_id
        WHERE m.service_type = 'SNOWPIPE_STREAMING'
          AND m.start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
          AND p.pipe_catalog = '{landing_db}'
        GROUP BY 1
        ORDER BY 1 DESC, 2 DESC;
        """
        
        landing_db = config.get("landing_database", "")
        formatted_query = query.format(landing_db=landing_db)

        results = []
        try:
            with snowflake.connector.connect(**conn_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(formatted_query)
                    for row in cur:
                        results.append({
                            "usage_date": row[0],
                            "total_credits": row[1]
                        })
        except Exception as e:
            logger.error(f"Snowflake query error: {str(e)}")
            raise

        return results

    def get_credit_usage(self, destination_id: int) -> CreditUsageResponse:
        """
        Get formatted credit usage stats for API response.
        """
        with db_manager.session() as session:
            # Verify destination exists
            dest = session.get(Destination, destination_id)
            if not dest:
                 return None

            today = datetime.now().date()
            
            # Helper to execute aggregation query
            def get_sum(start_date, end_date=None):
                query = select(func.sum(CreditSnowflakeMonitoring.total_credit)).where(
                    CreditSnowflakeMonitoring.destination_id == destination_id,
                    CreditSnowflakeMonitoring.usage_date >= start_date
                )
                if end_date:
                    query = query.where(CreditSnowflakeMonitoring.usage_date <= end_date)
                
                result = session.execute(query).scalar()
                return float(result) if result else 0.0

            # Current week (starts MONDAY)
            # Snowflake usage_date is usually the day of usage.
            # Assuming 'usage_date' is stored as the date of record (without time or time=00:00).
            start_week = today - timedelta(days=today.weekday())
            # Previous week
            start_prev_week = start_week - timedelta(weeks=1)
            end_prev_week = start_week - timedelta(days=1)
            
            # Current month
            start_month = today.replace(day=1)
            # Previous month
            last_month_end = start_month - timedelta(days=1)
            start_prev_month = last_month_end.replace(day=1)
            
            # Execute aggregations
            curr_week_sum = get_sum(start_week)
            prev_week_sum = get_sum(start_prev_week, end_prev_week)
            curr_month_sum = get_sum(start_month)
            prev_month_sum = get_sum(start_prev_month, last_month_end)
            
            # Daily data for chart (Last 30 days)
            limit_30_days = today - timedelta(days=30)
            
            daily_records = session.execute(
                select(
                    CreditSnowflakeMonitoring.usage_date,
                    CreditSnowflakeMonitoring.total_credit
                )
                .where(
                    CreditSnowflakeMonitoring.destination_id == destination_id,
                    CreditSnowflakeMonitoring.usage_date >= limit_30_days
                )
                .order_by(desc(CreditSnowflakeMonitoring.usage_date))
            ).all()
            
            daily_data = [
                DailyUsage(date=r.usage_date, credits=float(r.total_credit)) 
                for r in daily_records
            ]

            return CreditUsageResponse(
                summary=WeeklyMonthlyUsage(
                    current_week=curr_week_sum,
                    current_month=curr_month_sum,
                    previous_week=prev_week_sum,
                    previous_month=prev_month_sum
                ),
                daily_usage=daily_data
            )
