"""
Unit tests for data processing operations.
"""

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from best_practices import DataProcessor, DatabaseManager
from config import DB_CONFIG

class TestDataProcessor(unittest.TestCase):
    """Test cases for DataProcessor class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.db_manager = DatabaseManager(DB_CONFIG)
        self.processor = DataProcessor(self.db_manager)
    
    def test_clean_data(self):
        """Test data cleaning functionality."""
        # Create test dataframe
        df = pd.DataFrame({
            'A': [1, 2, None, 4],
            'B': ['a', 'b', None, 'd'],
            'C': [1.1, 2.2, 3.3, None]
        })
        
        # Clean data
        cleaned_df = self.processor._clean_data(df)
        
        # Assertions
        self.assertFalse(cleaned_df.isnull().any().any())
        self.assertEqual(len(cleaned_df), len(df))
    
    @patch('pandas.read_csv')
    @patch('best_practices.DataProcessor._clean_data')
    def test_import_data(self, mock_clean_data, mock_read_csv):
        """Test data import functionality."""
        # Setup mocks
        mock_df = pd.DataFrame({'A': [1, 2, 3]})
        mock_read_csv.return_value = mock_df
        mock_clean_data.return_value = mock_df
        
        # Test import
        self.processor.import_data('test.csv', 'test_table')
        
        # Assertions
        mock_read_csv.assert_called_once_with('test.csv')
        mock_clean_data.assert_called_once_with(mock_df)

if __name__ == '__main__':
    unittest.main() 