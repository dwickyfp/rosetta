"""
Database connection utilities for Rosetta Compute Engine.

Provides connection pooling and session management for PostgreSQL.
"""

import logging
import time
from contextlib import contextmanager
from typing import Generator, Any

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

from core.exceptions import DatabaseException

logger = logging.getLogger(__name__)

# Global connection pool
_connection_pool: pool.ThreadedConnectionPool | None = None


def init_connection_pool(
    min_conn: int = 1, max_conn: int = 10
) -> pool.ThreadedConnectionPool:
    """
    Initialize the connection pool.

    Args:
        min_conn: Minimum number of connections to maintain
        max_conn: Maximum number of connections allowed

    Returns:
        The initialized connection pool
    """
    global _connection_pool

    if _connection_pool is not None:
        return _connection_pool

    from config import get_config

    config = get_config()

    # Retry logic for Docker startup timing issues
    max_retries = 5
    retry_delay = 2.0
    last_error = None

    for attempt in range(max_retries):
        try:
            # Add keepalive and timeout settings to prevent connection drops
            dsn = config.database.dsn.copy()
            dsn.update(
                {
                    "connect_timeout": 10,
                    "keepalives": 1,
                    "keepalives_idle": 30,
                    "keepalives_interval": 10,
                    "keepalives_count": 5,
                    # Reduce statement timeout to prevent long-running queries from holding connections
                    "options": "-c statement_timeout=30000",  # 30 seconds
                }
            )

            _connection_pool = pool.ThreadedConnectionPool(
                minconn=min_conn, maxconn=max_conn, **dsn
            )
            logger.info(
                f"Connection pool initialized successfully on attempt {attempt + 1}"
            )
            return _connection_pool
        except psycopg2.Error as e:
            last_error = e
            if attempt < max_retries - 1:
                logger.warning(
                    f"Failed to initialize connection pool (attempt {attempt + 1}/{max_retries}): {e}. "
                    f"Retrying in {retry_delay:.1f}s..."
                )
                time.sleep(retry_delay)
                retry_delay *= 1.5  # Exponential backoff: 2s, 3s, 4.5s, 6.75s, 10s
            else:
                logger.error(
                    f"Failed to initialize connection pool after {max_retries} attempts: {e}"
                )

    raise DatabaseException(f"Failed to initialize connection pool: {last_error}")


def get_connection_pool() -> pool.ThreadedConnectionPool:
    """Get or create the connection pool."""
    global _connection_pool

    if _connection_pool is None:
        return init_connection_pool()

    return _connection_pool


def log_pool_stats() -> None:
    """Log connection pool statistics for debugging."""
    try:
        connection_pool = get_connection_pool()
        # Access private attributes to get pool stats
        # minconn and maxconn are public
        logger.info(
            f"Connection pool stats: "
            f"min={connection_pool.minconn}, "
            f"max={connection_pool.maxconn}, "
            f"closed={connection_pool.closed}"
        )
    except Exception as e:
        logger.warning(f"Failed to log pool stats: {e}")


def close_connection_pool() -> None:
    """Close the connection pool."""
    global _connection_pool

    if _connection_pool is not None:
        _connection_pool.closeall()
        _connection_pool = None


def get_db_connection() -> psycopg2.extensions.connection:
    """
    Get a database connection from the pool.

    Returns:
        A PostgreSQL connection

    Note:
        Caller is responsible for returning the connection to the pool.
    """
    connection_pool = get_connection_pool()
    max_retries = 5  # Increased from 3 to handle high concurrency
    retry_delay = 1.0  # Start with 1 second (increased from 0.5s)

    for attempt in range(max_retries):
        try:
            # Block up to 10 seconds waiting for available connection
            conn = connection_pool.getconn()

            # CRITICAL: Reset connection state before use
            # Connections returned from pool may still be in transaction state
            try:
                # Rollback any pending transactions
                if conn.status != psycopg2.extensions.STATUS_READY:
                    conn.rollback()

                # Use autocommit mode for health check to avoid starting a transaction
                conn.autocommit = True

                # Validate connection is alive with a simple query
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")  # Simple health check in autocommit mode

                # Reset to default non-autocommit mode for normal operations
                conn.autocommit = False

            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                # Connection is dead, remove it and get a new one
                logger.warning(f"Detected dead connection, removing from pool: {e}")
                connection_pool.putconn(conn, close=True)
                conn = connection_pool.getconn()

                # Reset the new connection too
                if conn.status != psycopg2.extensions.STATUS_READY:
                    conn.rollback()
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                conn.autocommit = False

            return conn
        except psycopg2.pool.PoolError as e:
            if attempt < max_retries - 1:
                logger.warning(
                    f"Connection pool exhausted (attempt {attempt + 1}/{max_retries}), "
                    f"retrying in {retry_delay:.2f}s..."
                )
                time.sleep(retry_delay)
                retry_delay *= 1.5  # Exponential backoff: 1s, 1.5s, 2.25s, 3.38s, 5.06s
            else:
                logger.error(
                    f"Failed to get connection after {max_retries} attempts: {e}"
                )
                raise DatabaseException(
                    f"Failed to get connection from pool (exhausted): {e}"
                )
        except psycopg2.Error as e:
            raise DatabaseException(f"Failed to get connection from pool: {e}")


def return_db_connection(conn: psycopg2.extensions.connection) -> None:
    """Return a connection to the pool after cleaning up transaction state."""
    pool = get_connection_pool()

    try:
        # CRITICAL: Clean up transaction state before returning to pool
        # Rollback any uncommitted transactions
        if conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION:
            conn.rollback()
            logger.debug(
                "Rolled back uncommitted transaction before returning connection to pool"
            )

        # Reset autocommit to default (False)
        conn.autocommit = False

    except Exception as e:
        logger.warning(f"Error cleaning up connection state: {e}. Closing connection.")
        # If cleanup fails, close the connection instead of returning it
        pool.putconn(conn, close=True)
        return

    # Return cleaned connection to pool
    pool.putconn(conn)


class DatabaseSession:
    """
    Context manager for database sessions with automatic transaction handling.

    Usage:
        with DatabaseSession() as session:
            result = session.execute("SELECT * FROM sources")
            rows = session.fetchall()
    """

    def __init__(self, autocommit: bool = False):
        """
        Initialize session.

        Args:
            autocommit: If True, disable transaction management
        """
        self._conn: psycopg2.extensions.connection | None = None
        self._cursor: RealDictCursor | None = None
        self._autocommit = autocommit
        self._has_writes = False  # Track if any write operations occurred

    def __enter__(self) -> "DatabaseSession":
        """Acquire connection and cursor."""
        self._conn = get_db_connection()
        self._conn.autocommit = self._autocommit
        self._cursor = self._conn.cursor(cursor_factory=RealDictCursor)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Handle transaction commit/rollback and cleanup."""
        connection_valid = True

        try:
            # Close cursor first to ensure all results are consumed
            if self._cursor:
                try:
                    self._cursor.close()
                    self._cursor = None
                except Exception as e:
                    logger.warning(f"Error closing cursor: {e}")

            if exc_type is not None:
                # Rollback on exception
                if self._conn and not self._autocommit:
                    try:
                        self._conn.rollback()
                        logger.warning(f"Transaction rolled back due to: {exc_val}")
                    except (
                        psycopg2.OperationalError,
                        psycopg2.InterfaceError,
                        psycopg2.DatabaseError,
                    ) as e:
                        logger.error(
                            f"Failed to rollback transaction (connection may be closed): {e}"
                        )
                        connection_valid = False
            else:
                # Only commit if there were write operations
                if self._conn and not self._autocommit:
                    if self._has_writes:
                        try:
                            self._conn.commit()
                        except (
                            psycopg2.OperationalError,
                            psycopg2.InterfaceError,
                            psycopg2.DatabaseError,
                        ) as e:
                            logger.error(
                                f"Failed to commit transaction (connection may be closed): {e}"
                            )
                            connection_valid = False
                            raise DatabaseException(f"Commit failed: {e}")
                    else:
                        # No writes - rollback to clean up read-only transaction
                        try:
                            self._conn.rollback()
                            logger.debug("Rolled back read-only transaction")
                        except Exception as e:
                            logger.warning(
                                f"Failed to rollback read-only transaction: {e}"
                            )
                            connection_valid = False
        finally:
            # Return connection to pool
            if self._conn:
                # If connection is invalid, close it instead of returning to pool
                if not connection_valid:
                    try:
                        pool = get_connection_pool()
                        pool.putconn(self._conn, close=True)
                        logger.warning(
                            "Closed invalid connection instead of returning to pool"
                        )
                    except Exception as e:
                        logger.error(f"Error closing invalid connection: {e}")
                else:
                    return_db_connection(self._conn)

        return False  # Don't suppress exceptions

    def execute(
        self, query: str, params: tuple | dict | None = None
    ) -> "DatabaseSession":
        """
        Execute a query.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            Self for method chaining
        """
        if self._cursor is None:
            raise DatabaseException("Session not initialized. Use as context manager.")

        try:
            self._cursor.execute(query, params)

            # Track if this is a write operation
            query_upper = query.strip().upper()
            if query_upper.startswith(
                ("INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP")
            ):
                self._has_writes = True

            return self
        except psycopg2.Error as e:
            raise DatabaseException(f"Query execution failed: {e}", {"query": query})

    def executemany(self, query: str, params_list: list) -> "DatabaseSession":
        """
        Execute a query with multiple parameter sets.

        Args:
            query: SQL query string
            params_list: List of parameter tuples/dicts

        Returns:
            Self for method chaining
        """
        if self._cursor is None:
            raise DatabaseException("Session not initialized. Use as context manager.")

        try:
            self._cursor.executemany(query, params_list)
            # executemany is typically used for writes
            self._has_writes = True
            return self
        except psycopg2.Error as e:
            raise DatabaseException(f"Batch execution failed: {e}", {"query": query})

    def fetchone(self) -> dict | None:
        """Fetch one row as dict."""
        if self._cursor is None:
            raise DatabaseException("Session not initialized.")
        try:
            return self._cursor.fetchone()
        except psycopg2.ProgrammingError as e:
            # Handle "no results to fetch" error
            if "no results to fetch" in str(e):
                return None
            raise DatabaseException(f"Error fetching result: {e}")

    def fetchall(self) -> list[dict]:
        """Fetch all rows as list of dicts."""
        if self._cursor is None:
            raise DatabaseException("Session not initialized.")
        try:
            return self._cursor.fetchall()
        except psycopg2.ProgrammingError as e:
            # Handle "no results to fetch" error
            if "no results to fetch" in str(e):
                return []
            raise DatabaseException(f"Error fetching results: {e}")

    def fetchmany(self, size: int) -> list[dict]:
        """Fetch specified number of rows."""
        if self._cursor is None:
            raise DatabaseException("Session not initialized.")
        return self._cursor.fetchmany(size)

    @property
    def rowcount(self) -> int:
        """Get number of affected rows."""
        if self._cursor is None:
            return 0
        return self._cursor.rowcount

    @property
    def lastrowid(self) -> int | None:
        """Get last inserted row ID (if available)."""
        if self._cursor is None:
            return None
        # PostgreSQL doesn't support lastrowid directly
        # Use RETURNING clause in INSERT instead
        return None


@contextmanager
def transaction() -> Generator[DatabaseSession, None, None]:
    """
    Context manager for explicit transaction handling.

    Usage:
        with transaction() as session:
            session.execute("INSERT INTO ...")
            session.execute("UPDATE ...")
    """
    with DatabaseSession() as session:
        yield session
