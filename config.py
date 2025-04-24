"""
Configuration settings for the project.
This file should be kept out of version control.
"""

import os
from typing import Dict, Any

# Database configuration
DB_CONFIG: Dict[str, str] = {
    'dbname': os.getenv('DB_NAME', 'training_db'),
    'user': os.getenv('DB_USER', 'training_user'),
    'password': os.getenv('DB_PASSWORD', 'password123'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

# Logging configuration
LOG_CONFIG: Dict[str, Any] = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'datefmt': '%Y-%m-%d %H:%M:%S'
}

# Data processing configuration
DATA_CONFIG: Dict[str, Any] = {
    'chunk_size': 1000,
    'default_encoding': 'utf-8',
    'na_values': ['NA', 'N/A', 'NULL', 'None', '']
} 