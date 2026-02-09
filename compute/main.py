#!/usr/bin/env python3
"""
Rosetta Compute Engine - Main Entry Point

Modular Debezium-based CDC engine for data streaming.

Configuration via environment variables:
    PIPELINE_ID     - Optional: Run specific pipeline by ID
    DEBUG           - Enable debug logging (true/false)
    LOG_LEVEL       - Logging level (DEBUG, INFO, WARNING, ERROR)
"""

import logging
import os
import sys

from compute.config import get_config
from compute.core.database import init_connection_pool, close_connection_pool, get_db_connection, return_db_connection
from compute.core.manager import PipelineManager
from compute.core.engine import run_pipeline


def run_migration(logger: logging.Logger) -> None:
    """Run database migration on startup."""
    migration_file = os.path.join(os.getcwd(), 'migrations', '001_create_table.sql')
    
    if not os.path.exists(migration_file):
        logger.warning(f"Migration file not found: {migration_file}")
        return

    logger.info(f"Running migration from {migration_file}")
    
    conn = None
    try:
        with open(migration_file, 'r') as f:
            sql_script = f.read()
            
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(sql_script)
            conn.commit()
            
        logger.info("Migration completed successfully")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        if conn:
            conn.rollback()
        # We might want to exit here if migration fails, but for now just log it
        raise e
    finally:
        if conn:
            return_db_connection(conn)


def setup_logging() -> None:
    """Configure logging based on environment and config."""
    config = get_config()

    # Check for DEBUG environment variable
    debug = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
    level = logging.DEBUG if debug else getattr(logging, config.logging.level.upper())

    logging.basicConfig(
        level=level,
        format=config.logging.format,
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )

    # Reduce noise from some libraries
    logging.getLogger("jpype").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def main() -> int:
    """Main entry point."""
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    init_connection_pool()

    try:
        logger.info("Starting Rosetta Compute Engine")
        logger.info("Press Ctrl+C to shutdown gracefully")

        # Running Migration SQL
        run_migration(logger)

        manager = PipelineManager()
        manager.run()
        logger.info("Shutdown complete")
        return 0

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1
    finally:
        close_connection_pool()


if __name__ == "__main__":
    sys.exit(main())
