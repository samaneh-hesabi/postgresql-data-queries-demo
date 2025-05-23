"""
Script to run the ETL process for the sales data warehouse.
"""
from utils.etl_utils import run_etl

if __name__ == '__main__':
    print("Starting ETL process for Sales Data Warehouse...")
    run_etl()
    print("ETL process completed successfully!") 