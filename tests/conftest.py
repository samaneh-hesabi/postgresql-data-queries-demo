"""
Common test fixtures and configuration.
"""
import os
import pytest
from pathlib import Path
from dotenv import load_dotenv

# Load test environment variables
load_dotenv('.env.test')

@pytest.fixture(scope='session')
def test_data_dir():
    """Return the path to the test data directory."""
    return Path(__file__).parent / 'data'

@pytest.fixture(scope='session')
def test_db_config():
    """Return test database configuration."""
    return {
        'dbname': os.getenv('TEST_DB_NAME', 'test_titanic_db'),
        'user': os.getenv('TEST_DB_USER', 'postgres'),
        'password': os.getenv('TEST_DB_PASSWORD', 'postgres'),
        'host': os.getenv('TEST_DB_HOST', 'localhost'),
        'port': os.getenv('TEST_DB_PORT', '5432')
    } 