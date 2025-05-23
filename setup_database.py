"""
Script to set up the sales data warehouse database.
"""
from utils.db_setup import setup_database

if __name__ == '__main__':
    print("Starting database setup for Sales Data Warehouse...")
    setup_database()
    print("Database setup completed successfully!") 