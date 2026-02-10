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
import threading
import time


from config import get_config
from core.database import init_connection_pool, close_connection_pool, get_db_connection, return_db_connection
from core.manager import PipelineManager
from core.engine import run_pipeline
from server import run_server



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
    config = get_config()
    
    manager = None

    try:
        logger.info("Starting Rosetta Compute Engine")
        logger.info("Press Ctrl+C to shutdown gracefully")

        # Running Migration SQL
        run_migration(logger)

        # Start API Server in a separate thread
        server_thread = threading.Thread(
            target=run_server,
            args=(config.server.host, config.server.port),
            daemon=True
        )
        server_thread.start()
        logger.info(f"API Server started at http://{config.server.host}:{config.server.port}")

        # Start Pipeline Manager in a separate thread
        manager = PipelineManager(register_signals=False)
        manager_thread = threading.Thread(
            target=manager.run,
            daemon=True
        )
        manager_thread.start()
        logger.info("Pipeline Manager started in background thread")

        # Keep main thread alive to handle signals and facilitate clean shutdown
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Shutdown requested via KeyboardInterrupt")
        return 0
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1
    finally:
        # If manager exists, try to shutdown gracefully (if not already handled by its own signals)
        # Note: manager.run() handles signals, but if we are here via KeyboardInterrupt caught in main,
        # we might want to ensure manager stops.
        # Since threads are daemon, they will be killed when main exits, 
        # but manager.shutdown() is cleaner if possible.
        if manager:
            logger.info("Shutting down Pipeline Manager...")
            manager.shutdown()
            
        close_connection_pool()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    sys.exit(main())
