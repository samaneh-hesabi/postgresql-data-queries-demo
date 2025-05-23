"""
Tests for postgresql_examples.py
"""
import pytest
import psycopg2
from postgresql_examples import create_tables, load_data

def test_create_tables(test_db_config):
    """Test table creation."""
    conn = psycopg2.connect(**test_db_config)
    try:
        create_tables(conn)
        with conn.cursor() as cur:
            # Check if tables exist
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = [table[0] for table in cur.fetchall()]
            assert 'passengers' in tables
    finally:
        conn.close()

def test_load_data(test_db_config, test_data_dir):
    """Test data loading."""
    conn = psycopg2.connect(**test_db_config)
    try:
        create_tables(conn)
        load_data(conn, test_data_dir / 'titanic.json')
        
        with conn.cursor() as cur:
            # Check if data was loaded
            cur.execute("SELECT COUNT(*) FROM passengers")
            count = cur.fetchone()[0]
            assert count > 0
    finally:
        conn.close() 