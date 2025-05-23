"""
Database utilities for the project.
"""
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, Any, Optional
from contextlib import contextmanager
from config import DB_CONFIG
from utils.logging_utils import setup_logger

logger = setup_logger(__name__)

@contextmanager
def get_db_connection():
    """
    Context manager for database connections.
    
    Yields:
        psycopg2.connection: Database connection
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        yield conn
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        if conn is not None:
            conn.close()

def execute_query(query: str, params: Optional[Dict[str, Any]] = None) -> list:
    """
    Execute a query and return results.
    
    Args:
        query: SQL query to execute
        params: Optional parameters for the query
    
    Returns:
        list: Query results
    """
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(query, params)
                if cur.description:  # If query returns results
                    return cur.fetchall()
                return []
            except psycopg2.Error as e:
                logger.error(f"Query execution error: {e}")
                raise 