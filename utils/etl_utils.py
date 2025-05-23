"""
ETL utilities for the sales data warehouse.
"""
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from typing import List, Dict, Any
import os
from config import DB_CONFIG, SCHEMA_CONFIG, RAW_DATA_DIR

def load_csv_to_staging(csv_file: str, staging_table: str) -> None:
    """
    Load data from CSV file to staging table.
    
    Args:
        csv_file: Name of the CSV file in raw data directory
        staging_table: Name of the staging table
    """
    print(f"Loading {csv_file} to staging table {staging_table}...")
    
    # Read CSV file
    df = pd.read_csv(os.path.join(RAW_DATA_DIR, csv_file))
    
    # Connect to database
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Clear existing data in staging table
    cur.execute(f"TRUNCATE TABLE {SCHEMA_CONFIG['staging_schema']}.{staging_table}")
    
    # Convert DataFrame to list of tuples for bulk insert
    data = [tuple(x) for x in df.values]
    columns = df.columns.tolist()
    
    # Create the INSERT query
    insert_query = f"""
    INSERT INTO {SCHEMA_CONFIG['staging_schema']}.{staging_table} 
    ({', '.join(columns)})
    VALUES %s
    """
    
    # Execute bulk insert
    execute_values(cur, insert_query, data)
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"Successfully loaded {len(data)} rows to {staging_table}")

def load_dimension_tables() -> None:
    """Load data from staging to dimension tables."""
    print("Loading dimension tables...")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Load Product Dimension
    cur.execute(f"""
    INSERT INTO {SCHEMA_CONFIG['dim_schema']}.dim_product
    SELECT DISTINCT * FROM {SCHEMA_CONFIG['staging_schema']}.stg_products
    ON CONFLICT (product_id) DO UPDATE SET
        product_name = EXCLUDED.product_name,
        category = EXCLUDED.category,
        subcategory = EXCLUDED.subcategory,
        brand = EXCLUDED.brand,
        unit_price = EXCLUDED.unit_price,
        cost = EXCLUDED.cost,
        modified_date = EXCLUDED.modified_date
    """)
    
    # Load Customer Dimension
    cur.execute(f"""
    INSERT INTO {SCHEMA_CONFIG['dim_schema']}.dim_customer
    SELECT DISTINCT * FROM {SCHEMA_CONFIG['staging_schema']}.stg_customers
    ON CONFLICT (customer_id) DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        email = EXCLUDED.email,
        phone = EXCLUDED.phone,
        address = EXCLUDED.address,
        city = EXCLUDED.city,
        state = EXCLUDED.state,
        country = EXCLUDED.country,
        postal_code = EXCLUDED.postal_code,
        customer_segment = EXCLUDED.customer_segment,
        modified_date = EXCLUDED.modified_date
    """)
    
    # Load Time Dimension
    cur.execute(f"""
    INSERT INTO {SCHEMA_CONFIG['dim_schema']}.dim_time
    SELECT DISTINCT * FROM {SCHEMA_CONFIG['staging_schema']}.stg_time_dimension
    ON CONFLICT (date_id) DO UPDATE SET
        full_date = EXCLUDED.full_date,
        day_of_week = EXCLUDED.day_of_week,
        day_of_month = EXCLUDED.day_of_month,
        day_of_year = EXCLUDED.day_of_year,
        week_of_year = EXCLUDED.week_of_year,
        month = EXCLUDED.month,
        quarter = EXCLUDED.quarter,
        year = EXCLUDED.year,
        is_holiday = EXCLUDED.is_holiday,
        holiday_name = EXCLUDED.holiday_name
    """)
    
    # Load Store Dimension
    cur.execute(f"""
    INSERT INTO {SCHEMA_CONFIG['dim_schema']}.dim_store
    SELECT DISTINCT * FROM {SCHEMA_CONFIG['staging_schema']}.stg_stores
    ON CONFLICT (store_id) DO UPDATE SET
        store_name = EXCLUDED.store_name,
        address = EXCLUDED.address,
        city = EXCLUDED.city,
        state = EXCLUDED.state,
        country = EXCLUDED.country,
        postal_code = EXCLUDED.postal_code,
        manager = EXCLUDED.manager,
        opening_date = EXCLUDED.opening_date,
        store_type = EXCLUDED.store_type,
        store_size = EXCLUDED.store_size,
        modified_date = EXCLUDED.modified_date
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Dimension tables loaded successfully.")

def load_fact_tables() -> None:
    """Load data from staging to fact tables."""
    print("Loading fact tables...")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Load Sales Fact
    cur.execute(f"""
    INSERT INTO {SCHEMA_CONFIG['fact_schema']}.fact_sales
    SELECT * FROM {SCHEMA_CONFIG['staging_schema']}.stg_sales
    ON CONFLICT (sale_id) DO UPDATE SET
        date_id = EXCLUDED.date_id,
        product_id = EXCLUDED.product_id,
        customer_id = EXCLUDED.customer_id,
        store_id = EXCLUDED.store_id,
        quantity = EXCLUDED.quantity,
        unit_price = EXCLUDED.unit_price,
        total_amount = EXCLUDED.total_amount,
        discount_amount = EXCLUDED.discount_amount,
        net_amount = EXCLUDED.net_amount,
        payment_method = EXCLUDED.payment_method,
        transaction_time = EXCLUDED.transaction_time
    """)
    
    # Load Inventory Fact
    cur.execute(f"""
    INSERT INTO {SCHEMA_CONFIG['fact_schema']}.fact_inventory
    SELECT * FROM {SCHEMA_CONFIG['staging_schema']}.stg_inventory
    ON CONFLICT (inventory_id) DO UPDATE SET
        date_id = EXCLUDED.date_id,
        product_id = EXCLUDED.product_id,
        store_id = EXCLUDED.store_id,
        beginning_quantity = EXCLUDED.beginning_quantity,
        ending_quantity = EXCLUDED.ending_quantity,
        units_received = EXCLUDED.units_received,
        units_sold = EXCLUDED.units_sold,
        units_damaged = EXCLUDED.units_damaged,
        reorder_point = EXCLUDED.reorder_point,
        reorder_quantity = EXCLUDED.reorder_quantity
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Fact tables loaded successfully.")

def run_etl():
    """Run the complete ETL process."""
    print("Starting ETL process...")
    
    # Load data to staging
    load_csv_to_staging('products.csv', 'stg_products')
    load_csv_to_staging('customers.csv', 'stg_customers')
    load_csv_to_staging('time_dimension.csv', 'stg_time_dimension')
    load_csv_to_staging('stores.csv', 'stg_stores')
    load_csv_to_staging('sales.csv', 'stg_sales')
    load_csv_to_staging('inventory.csv', 'stg_inventory')
    
    # Load dimension tables
    load_dimension_tables()
    
    # Load fact tables
    load_fact_tables()
    
    print("ETL process completed successfully!")

if __name__ == '__main__':
    run_etl() 