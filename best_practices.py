"""
Best Practices for PostgreSQL and Python Integration
This file demonstrates proper database handling, error management, and code organization.
"""

import logging
from typing import List, Dict, Any, Optional, Union
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor
from config import DB_CONFIG, LOG_CONFIG, DATA_CONFIG

# Configure logging
logging.basicConfig(**LOG_CONFIG)
logger = logging.getLogger(__name__)

class DatabaseError(Exception):
    """Custom exception for database-related errors."""
    pass

class DataProcessingError(Exception):
    """Custom exception for data processing-related errors."""
    pass

class DatabaseManager:
    """Handles database connections and operations with proper error handling."""
    
    def __init__(self, config: Dict[str, str]):
        """
        Initialize the database manager.
        
        Args:
            config: Dictionary containing database connection parameters
        """
        self.config = config
        try:
            self.engine = create_engine(
                f'postgresql://{config["user"]}:{config["password"]}@{config["host"]}:{config["port"]}/{config["dbname"]}'
            )
        except SQLAlchemyError as e:
            logger.error(f"Failed to create database engine: {e}")
            raise DatabaseError(f"Database engine creation failed: {e}")
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections with automatic cleanup.
        
        Yields:
            psycopg2.connection: Database connection object
            
        Raises:
            DatabaseError: If connection fails
        """
        conn = None
        try:
            conn = psycopg2.connect(**self.config)
            yield conn
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {e}")
            raise DatabaseError(f"Connection failed: {e}")
        finally:
            if conn:
                conn.close()
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Execute a query with proper error handling and parameterization.
        
        Args:
            query: SQL query string
            params: Optional dictionary of query parameters
            
        Returns:
            List of dictionaries containing query results
            
        Raises:
            DatabaseError: If query execution fails
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, params or {})
                    return cur.fetchall()
        except (psycopg2.Error, DatabaseError) as e:
            logger.error(f"Query execution error: {e}")
            raise DatabaseError(f"Query execution failed: {e}")
    
    def execute_many(self, query: str, params_list: List[Dict]) -> None:
        """
        Execute multiple queries with proper transaction handling.
        
        Args:
            query: SQL query string
            params_list: List of parameter dictionaries
            
        Raises:
            DatabaseError: If batch execution fails
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    for params in params_list:
                        cur.execute(query, params)
                conn.commit()
        except (psycopg2.Error, DatabaseError) as e:
            conn.rollback()
            logger.error(f"Batch execution error: {e}")
            raise DatabaseError(f"Batch execution failed: {e}")

class DataProcessor:
    """Handles data processing operations with proper validation."""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize the data processor.
        
        Args:
            db_manager: DatabaseManager instance
        """
        self.db = db_manager
    
    def import_data(self, file_path: str, table_name: str) -> None:
        """
        Import data from CSV with proper validation and error handling.
        
        Args:
            file_path: Path to the CSV file
            table_name: Name of the target table
            
        Raises:
            DataProcessingError: If data import fails
        """
        try:
            # Read CSV with validation
            df = pd.read_csv(
                file_path,
                encoding=DATA_CONFIG['default_encoding'],
                na_values=DATA_CONFIG['na_values']
            )
            
            # Data validation
            if df.empty:
                raise DataProcessingError("Empty dataframe")
            
            # Data cleaning
            df = self._clean_data(df)
            
            # Import to database
            with self.db.engine.connect() as conn:
                df.to_sql(
                    table_name,
                    conn,
                    if_exists='replace',
                    index=False,
                    method='multi',
                    chunksize=DATA_CONFIG['chunk_size']
                )
            
            logger.info(f"Successfully imported data to {table_name}")
            
        except Exception as e:
            logger.error(f"Data import error: {e}")
            raise DataProcessingError(f"Data import failed: {e}")
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and validate data before import.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Handle missing values
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].fillna('Unknown')
            else:
                df[col] = df[col].fillna(df[col].median())
        
        return df

def analyze_titanic_data():
    """Example analysis using best practices."""
    try:
        # Initialize database manager
        db = DatabaseManager(DB_CONFIG)
        processor = DataProcessor(db)
        
        # Import data
        processor.import_data('data/titanic.csv', 'titanic')
        
        # Example queries with proper error handling
        queries = [
            {
                'name': 'Survival by Class',
                'query': """
                    SELECT 
                        "Pclass",
                        COUNT(*) as total,
                        SUM(CASE WHEN "Survived" = 1 THEN 1 ELSE 0 END) as survivors,
                        ROUND(CAST(SUM(CASE WHEN "Survived" = 1 THEN 1 ELSE 0 END) AS numeric) / COUNT(*) * 100, 2) as survival_rate
                    FROM titanic
                    GROUP BY "Pclass"
                    ORDER BY "Pclass"
                """
            },
            {
                'name': 'Age Distribution',
                'query': """
                    SELECT 
                        CASE 
                            WHEN "Age" < 18 THEN 'Child'
                            WHEN "Age" < 30 THEN 'Young Adult'
                            WHEN "Age" < 50 THEN 'Adult'
                            ELSE 'Senior'
                        END as age_group,
                        COUNT(*) as count,
                        ROUND(CAST(AVG("Fare") AS numeric), 2) as avg_fare
                    FROM titanic
                    GROUP BY age_group
                    ORDER BY count DESC
                """
            }
        ]
        
        # Execute and log results
        for query_info in queries:
            try:
                results = db.execute_query(query_info['query'])
                logger.info(f"\n{query_info['name']} Results:")
                for row in results:
                    logger.info(row)
            except DatabaseError as e:
                logger.error(f"Error executing {query_info['name']}: {e}")
        
    except Exception as e:
        logger.error(f"Analysis error: {e}")
        raise

if __name__ == "__main__":
    analyze_titanic_data() 