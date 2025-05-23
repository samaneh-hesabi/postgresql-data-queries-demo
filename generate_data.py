"""
Script to generate sample data for the sales data warehouse.
"""
from utils.data_generator import generate_all_data

if __name__ == '__main__':
    print("Starting data generation for Sales Data Warehouse...")
    generate_all_data()
    print("Data generation completed successfully!") 