"""
Sample analytics queries for the sales data warehouse.
"""
from typing import List, Dict, Any
import psycopg2
from config import DB_CONFIG, SCHEMA_CONFIG
from datetime import datetime, date
from decimal import Decimal
import json

def get_connection():
    """Get database connection."""
    return psycopg2.connect(**DB_CONFIG)

def execute_query(query: str) -> List[Dict[str, Any]]:
    """
    Execute a query and return results as a list of dictionaries.
    
    Args:
        query: SQL query to execute
        
    Returns:
        List of dictionaries containing query results
    """
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute(query)
    columns = [desc[0] for desc in cur.description]
    results = [dict(zip(columns, row)) for row in cur.fetchall()]
    
    cur.close()
    conn.close()
    return results

def get_daily_sales_by_store() -> List[Dict[str, Any]]:
    """Get daily sales totals by store."""
    query = f"""
    SELECT 
        s.store_name,
        t.full_date,
        SUM(fs.net_amount) as total_sales,
        COUNT(fs.sale_id) as number_of_transactions,
        AVG(fs.net_amount) as average_transaction_value
    FROM {SCHEMA_CONFIG['fact_schema']}.fact_sales fs
    JOIN {SCHEMA_CONFIG['dim_schema']}.dim_store s ON fs.store_id = s.store_id
    JOIN {SCHEMA_CONFIG['dim_schema']}.dim_time t ON fs.date_id = t.date_id
    GROUP BY s.store_name, t.full_date
    ORDER BY t.full_date, total_sales DESC
    """
    return execute_query(query)

def get_product_performance() -> List[Dict[str, Any]]:
    """Get product performance metrics."""
    query = f"""
    SELECT 
        p.product_name,
        p.category,
        p.brand,
        COUNT(fs.sale_id) as total_sales,
        SUM(fs.quantity) as total_quantity_sold,
        SUM(fs.net_amount) as total_revenue,
        AVG(fs.unit_price) as average_price
    FROM {SCHEMA_CONFIG['fact_schema']}.fact_sales fs
    JOIN {SCHEMA_CONFIG['dim_schema']}.dim_product p ON fs.product_id = p.product_id
    GROUP BY p.product_name, p.category, p.brand
    ORDER BY total_revenue DESC
    """
    return execute_query(query)

def get_customer_segment_analysis() -> List[Dict[str, Any]]:
    """Get customer segment analysis."""
    query = f"""
    SELECT 
        c.customer_segment,
        COUNT(DISTINCT c.customer_id) as number_of_customers,
        SUM(fs.net_amount) as total_revenue,
        AVG(fs.net_amount) as average_revenue_per_customer,
        COUNT(fs.sale_id) as total_transactions
    FROM {SCHEMA_CONFIG['fact_schema']}.fact_sales fs
    JOIN {SCHEMA_CONFIG['dim_schema']}.dim_customer c ON fs.customer_id = c.customer_id
    GROUP BY c.customer_segment
    ORDER BY total_revenue DESC
    """
    return execute_query(query)

def get_inventory_analysis() -> List[Dict[str, Any]]:
    """Get inventory analysis by store and product category."""
    query = f"""
    SELECT 
        s.store_name,
        p.category,
        SUM(fi.ending_quantity) as current_stock,
        SUM(fi.units_sold) as total_sold,
        SUM(fi.units_damaged) as total_damaged,
        AVG(fi.reorder_point) as average_reorder_point
    FROM {SCHEMA_CONFIG['fact_schema']}.fact_inventory fi
    JOIN {SCHEMA_CONFIG['dim_schema']}.dim_store s ON fi.store_id = s.store_id
    JOIN {SCHEMA_CONFIG['dim_schema']}.dim_product p ON fi.product_id = p.product_id
    GROUP BY s.store_name, p.category
    ORDER BY s.store_name, p.category
    """
    return execute_query(query)

def get_sales_trends() -> List[Dict[str, Any]]:
    """Get sales trends by month and category."""
    query = f"""
    SELECT 
        t.year,
        t.month,
        p.category,
        SUM(fs.net_amount) as total_sales,
        COUNT(fs.sale_id) as number_of_transactions,
        AVG(fs.net_amount) as average_transaction_value
    FROM {SCHEMA_CONFIG['fact_schema']}.fact_sales fs
    JOIN {SCHEMA_CONFIG['dim_schema']}.dim_time t ON fs.date_id = t.date_id
    JOIN {SCHEMA_CONFIG['dim_schema']}.dim_product p ON fs.product_id = p.product_id
    GROUP BY t.year, t.month, p.category
    ORDER BY t.year, t.month, total_sales DESC
    """
    return execute_query(query)

def get_top_performing_stores() -> List[Dict[str, Any]]:
    """Get top performing stores by revenue and transaction count."""
    query = f"""
    SELECT 
        s.store_name,
        s.store_type,
        s.city,
        s.state,
        COUNT(fs.sale_id) as total_transactions,
        SUM(fs.net_amount) as total_revenue,
        AVG(fs.net_amount) as average_transaction_value,
        COUNT(DISTINCT fs.customer_id) as unique_customers
    FROM {SCHEMA_CONFIG['fact_schema']}.fact_sales fs
    JOIN {SCHEMA_CONFIG['dim_schema']}.dim_store s ON fs.store_id = s.store_id
    GROUP BY s.store_name, s.store_type, s.city, s.state
    ORDER BY total_revenue DESC
    """
    return execute_query(query)

def get_customer_purchase_patterns() -> List[Dict[str, Any]]:
    """Get customer purchase patterns by time of day and day of week."""
    query = f"""
    SELECT 
        t.day_of_week,
        EXTRACT(HOUR FROM fs.transaction_time) as hour_of_day,
        COUNT(fs.sale_id) as number_of_transactions,
        SUM(fs.net_amount) as total_sales,
        AVG(fs.net_amount) as average_transaction_value
    FROM {SCHEMA_CONFIG['fact_schema']}.fact_sales fs
    JOIN {SCHEMA_CONFIG['dim_schema']}.dim_time t ON fs.date_id = t.date_id
    GROUP BY t.day_of_week, EXTRACT(HOUR FROM fs.transaction_time)
    ORDER BY t.day_of_week, hour_of_day
    """
    return execute_query(query)

def format_decimal(obj):
    """Helper function to format Decimal objects for JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj

if __name__ == '__main__':
    print("\n=== Sales Data Warehouse Analytics ===\n")
    
    print("1. Daily Sales by Store (Top 5):")
    daily_sales = get_daily_sales_by_store()[:5]
    print(json.dumps(daily_sales, indent=2, default=format_decimal))
    
    print("\n2. Product Performance (Top 5):")
    product_perf = get_product_performance()[:5]
    print(json.dumps(product_perf, indent=2, default=format_decimal))
    
    print("\n3. Customer Segment Analysis:")
    segment_analysis = get_customer_segment_analysis()
    print(json.dumps(segment_analysis, indent=2, default=format_decimal))
    
    print("\n4. Inventory Analysis (Top 5):")
    inventory = get_inventory_analysis()[:5]
    print(json.dumps(inventory, indent=2, default=format_decimal))
    
    print("\n5. Sales Trends (Top 5):")
    trends = get_sales_trends()[:5]
    print(json.dumps(trends, indent=2, default=format_decimal))
    
    print("\n6. Top Performing Stores (Top 5):")
    top_stores = get_top_performing_stores()[:5]
    print(json.dumps(top_stores, indent=2, default=format_decimal))
    
    print("\n7. Customer Purchase Patterns (Sample):")
    patterns = get_customer_purchase_patterns()[:5]
    print(json.dumps(patterns, indent=2, default=format_decimal)) 