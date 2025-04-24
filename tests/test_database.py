"""
Unit tests for database operations.
"""

import unittest
from unittest.mock import patch, MagicMock
from best_practices import DatabaseManager
from config import DB_CONFIG

class TestDatabaseManager(unittest.TestCase):
    """Test cases for DatabaseManager class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.db_manager = DatabaseManager(DB_CONFIG)
    
    @patch('psycopg2.connect')
    def test_get_connection(self, mock_connect):
        """Test database connection."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        with self.db_manager.get_connection() as conn:
            self.assertEqual(conn, mock_conn)
        
        mock_connect.assert_called_once_with(**DB_CONFIG)
        mock_conn.close.assert_called_once()
    
    @patch('best_practices.DatabaseManager.get_connection')
    def test_execute_query(self, mock_get_connection):
        """Test query execution."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        query = "SELECT * FROM test_table"
        params = {'id': 1}
        
        self.db_manager.execute_query(query, params)
        
        mock_cursor.execute.assert_called_once_with(query, params)
        mock_cursor.fetchall.assert_called_once()

if __name__ == '__main__':
    unittest.main() 