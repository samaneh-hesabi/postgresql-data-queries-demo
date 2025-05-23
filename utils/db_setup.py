"""
Database setup script for the sales data warehouse.
"""
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config import DB_CONFIG, SCHEMA_CONFIG

def create_database():
    """Create the database if it doesn't exist."""
    conn = psycopg2.connect(
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port']
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    
    # Check if database exists
    cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (DB_CONFIG['dbname'],))
    exists = cur.fetchone()
    
    if not exists:
        cur.execute(f"CREATE DATABASE {DB_CONFIG['dbname']}")
        print(f"Database {DB_CONFIG['dbname']} created successfully.")
    else:
        print(f"Database {DB_CONFIG['dbname']} already exists.")
    
    cur.close()
    conn.close()

def create_schemas():
    """Create the necessary schemas in the database."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Create schemas
    schemas = [
        SCHEMA_CONFIG['schema_name'],
        SCHEMA_CONFIG['staging_schema'],
        SCHEMA_CONFIG['dim_schema'],
        SCHEMA_CONFIG['fact_schema']
    ]
    
    for schema in schemas:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
    conn.commit()
    cur.close()
    conn.close()
    print("Schemas created successfully.")

def create_dimension_tables():
    """Create dimension tables in the data warehouse."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Product Dimension
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_CONFIG['dim_schema']}.dim_product (
        product_id VARCHAR(10) PRIMARY KEY,
        product_name VARCHAR(100) NOT NULL,
        category VARCHAR(50) NOT NULL,
        subcategory VARCHAR(50) NOT NULL,
        brand VARCHAR(50) NOT NULL,
        unit_price DECIMAL(10,2) NOT NULL,
        cost DECIMAL(10,2) NOT NULL,
        created_date DATE NOT NULL,
        modified_date DATE NOT NULL
    )
    """)
    
    # Customer Dimension
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_CONFIG['dim_schema']}.dim_customer (
        customer_id VARCHAR(10) PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        email VARCHAR(100) NOT NULL,
        phone VARCHAR(20),
        address VARCHAR(100) NOT NULL,
        city VARCHAR(50) NOT NULL,
        state VARCHAR(50) NOT NULL,
        country VARCHAR(50) NOT NULL,
        postal_code VARCHAR(10) NOT NULL,
        customer_segment VARCHAR(20) NOT NULL,
        created_date DATE NOT NULL,
        modified_date DATE NOT NULL
    )
    """)
    
    # Time Dimension
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_CONFIG['dim_schema']}.dim_time (
        date_id VARCHAR(8) PRIMARY KEY,
        full_date DATE NOT NULL,
        day_of_week VARCHAR(10) NOT NULL,
        day_of_month INTEGER NOT NULL,
        day_of_year INTEGER NOT NULL,
        week_of_year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        quarter INTEGER NOT NULL,
        year INTEGER NOT NULL,
        is_holiday BOOLEAN NOT NULL,
        holiday_name VARCHAR(50)
    )
    """)
    
    # Store Dimension
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_CONFIG['dim_schema']}.dim_store (
        store_id VARCHAR(10) PRIMARY KEY,
        store_name VARCHAR(100) NOT NULL,
        address VARCHAR(100) NOT NULL,
        city VARCHAR(50) NOT NULL,
        state VARCHAR(50) NOT NULL,
        country VARCHAR(50) NOT NULL,
        postal_code VARCHAR(10) NOT NULL,
        manager VARCHAR(100) NOT NULL,
        opening_date DATE NOT NULL,
        store_type VARCHAR(20) NOT NULL,
        store_size DECIMAL(10,2) NOT NULL,
        created_date DATE NOT NULL,
        modified_date DATE NOT NULL
    )
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Dimension tables created successfully.")

def create_fact_tables():
    """Create fact tables in the data warehouse."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Sales Fact
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_CONFIG['fact_schema']}.fact_sales (
        sale_id VARCHAR(10) PRIMARY KEY,
        date_id VARCHAR(8) NOT NULL,
        product_id VARCHAR(10) NOT NULL,
        customer_id VARCHAR(10) NOT NULL,
        store_id VARCHAR(10) NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price DECIMAL(10,2) NOT NULL,
        total_amount DECIMAL(10,2) NOT NULL,
        discount_amount DECIMAL(10,2) NOT NULL,
        net_amount DECIMAL(10,2) NOT NULL,
        payment_method VARCHAR(20) NOT NULL,
        transaction_time TIMESTAMP NOT NULL,
        FOREIGN KEY (date_id) REFERENCES {SCHEMA_CONFIG['dim_schema']}.dim_time(date_id),
        FOREIGN KEY (product_id) REFERENCES {SCHEMA_CONFIG['dim_schema']}.dim_product(product_id),
        FOREIGN KEY (customer_id) REFERENCES {SCHEMA_CONFIG['dim_schema']}.dim_customer(customer_id),
        FOREIGN KEY (store_id) REFERENCES {SCHEMA_CONFIG['dim_schema']}.dim_store(store_id)
    )
    """)
    
    # Inventory Fact
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_CONFIG['fact_schema']}.fact_inventory (
        inventory_id VARCHAR(10) PRIMARY KEY,
        date_id VARCHAR(8) NOT NULL,
        product_id VARCHAR(10) NOT NULL,
        store_id VARCHAR(10) NOT NULL,
        beginning_quantity INTEGER NOT NULL,
        ending_quantity INTEGER NOT NULL,
        units_received INTEGER NOT NULL,
        units_sold INTEGER NOT NULL,
        units_damaged INTEGER NOT NULL,
        reorder_point INTEGER NOT NULL,
        reorder_quantity INTEGER NOT NULL,
        FOREIGN KEY (date_id) REFERENCES {SCHEMA_CONFIG['dim_schema']}.dim_time(date_id),
        FOREIGN KEY (product_id) REFERENCES {SCHEMA_CONFIG['dim_schema']}.dim_product(product_id),
        FOREIGN KEY (store_id) REFERENCES {SCHEMA_CONFIG['dim_schema']}.dim_store(store_id)
    )
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Fact tables created successfully.")

def create_staging_tables():
    """Create staging tables for ETL process."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Staging tables mirror the structure of the source files
    staging_tables = {
        'stg_products': """
            product_id VARCHAR(10),
            product_name VARCHAR(100),
            category VARCHAR(50),
            subcategory VARCHAR(50),
            brand VARCHAR(50),
            unit_price DECIMAL(10,2),
            cost DECIMAL(10,2),
            created_date DATE,
            modified_date DATE
        """,
        'stg_customers': """
            customer_id VARCHAR(10),
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(100),
            phone VARCHAR(20),
            address VARCHAR(100),
            city VARCHAR(50),
            state VARCHAR(50),
            country VARCHAR(50),
            postal_code VARCHAR(10),
            customer_segment VARCHAR(20),
            created_date DATE,
            modified_date DATE
        """,
        'stg_time_dimension': """
            date_id VARCHAR(8),
            full_date DATE,
            day_of_week VARCHAR(10),
            day_of_month INTEGER,
            day_of_year INTEGER,
            week_of_year INTEGER,
            month INTEGER,
            quarter INTEGER,
            year INTEGER,
            is_holiday BOOLEAN,
            holiday_name VARCHAR(50)
        """,
        'stg_stores': """
            store_id VARCHAR(10),
            store_name VARCHAR(100),
            address VARCHAR(100),
            city VARCHAR(50),
            state VARCHAR(50),
            country VARCHAR(50),
            postal_code VARCHAR(10),
            manager VARCHAR(100),
            opening_date DATE,
            store_type VARCHAR(20),
            store_size DECIMAL(10,2),
            created_date DATE,
            modified_date DATE
        """,
        'stg_sales': """
            sale_id VARCHAR(10),
            date_id VARCHAR(8),
            product_id VARCHAR(10),
            customer_id VARCHAR(10),
            store_id VARCHAR(10),
            quantity INTEGER,
            unit_price DECIMAL(10,2),
            total_amount DECIMAL(10,2),
            discount_amount DECIMAL(10,2),
            net_amount DECIMAL(10,2),
            payment_method VARCHAR(20),
            transaction_time TIMESTAMP
        """,
        'stg_inventory': """
            inventory_id VARCHAR(10),
            date_id VARCHAR(8),
            product_id VARCHAR(10),
            store_id VARCHAR(10),
            beginning_quantity INTEGER,
            ending_quantity INTEGER,
            units_received INTEGER,
            units_sold INTEGER,
            units_damaged INTEGER,
            reorder_point INTEGER,
            reorder_quantity INTEGER
        """
    }
    
    for table_name, columns in staging_tables.items():
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_CONFIG['staging_schema']}.{table_name} (
            {columns}
        )
        """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Staging tables created successfully.")

def setup_database():
    """Set up the complete database structure."""
    print("Setting up database...")
    create_database()
    create_schemas()
    create_dimension_tables()
    create_fact_tables()
    create_staging_tables()
    print("Database setup completed successfully!")

if __name__ == '__main__':
    setup_database() 