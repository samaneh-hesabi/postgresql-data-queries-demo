"""
Data generator utility for creating synthetic sales data warehouse data.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from typing import List, Dict, Any
import os
from config import RAW_DATA_DIR

# Constants for data generation
NUM_PRODUCTS = 1000
NUM_CUSTOMERS = 5000
NUM_STORES = 50
DAYS_OF_DATA = 365
PRODUCT_CATEGORIES = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Food', 'Beauty']
PAYMENT_METHODS = ['Credit Card', 'Debit Card', 'Cash', 'Mobile Payment']
STORE_TYPES = ['Mall', 'Standalone', 'Outlet', 'Supermarket']
CUSTOMER_SEGMENTS = ['Regular', 'Premium', 'VIP', 'Wholesale']

def generate_product_data() -> pd.DataFrame:
    """Generate product dimension data."""
    data = {
        'product_id': [f'P{i:04d}' for i in range(1, NUM_PRODUCTS + 1)],
        'product_name': [f'Product {i}' for i in range(1, NUM_PRODUCTS + 1)],
        'category': np.random.choice(PRODUCT_CATEGORIES, NUM_PRODUCTS),
        'subcategory': [f'Subcategory {i}' for i in range(1, NUM_PRODUCTS + 1)],
        'brand': [f'Brand {i % 50 + 1}' for i in range(NUM_PRODUCTS)],
        'unit_price': np.random.uniform(10, 1000, NUM_PRODUCTS).round(2),
        'cost': np.random.uniform(5, 500, NUM_PRODUCTS).round(2),
        'created_date': pd.date_range(start='2020-01-01', periods=NUM_PRODUCTS, freq='D'),
        'modified_date': pd.date_range(start='2020-01-01', periods=NUM_PRODUCTS, freq='D')
    }
    return pd.DataFrame(data)

def generate_customer_data() -> pd.DataFrame:
    """Generate customer dimension data."""
    data = {
        'customer_id': [f'C{i:04d}' for i in range(1, NUM_CUSTOMERS + 1)],
        'first_name': [f'First{i}' for i in range(1, NUM_CUSTOMERS + 1)],
        'last_name': [f'Last{i}' for i in range(1, NUM_CUSTOMERS + 1)],
        'email': [f'customer{i}@example.com' for i in range(1, NUM_CUSTOMERS + 1)],
        'phone': [f'+1{random.randint(1000000000, 9999999999)}' for _ in range(NUM_CUSTOMERS)],
        'address': [f'{random.randint(1, 9999)} Main St' for _ in range(NUM_CUSTOMERS)],
        'city': [f'City{i % 100 + 1}' for i in range(NUM_CUSTOMERS)],
        'state': [f'State{i % 50 + 1}' for i in range(NUM_CUSTOMERS)],
        'country': ['USA'] * NUM_CUSTOMERS,
        'postal_code': [f'{random.randint(10000, 99999)}' for _ in range(NUM_CUSTOMERS)],
        'customer_segment': np.random.choice(CUSTOMER_SEGMENTS, NUM_CUSTOMERS),
        'created_date': pd.date_range(start='2019-01-01', periods=NUM_CUSTOMERS, freq='D'),
        'modified_date': pd.date_range(start='2019-01-01', periods=NUM_CUSTOMERS, freq='D')
    }
    return pd.DataFrame(data)

def generate_time_dimension() -> pd.DataFrame:
    """Generate time dimension data."""
    start_date = datetime(2023, 1, 1)
    dates = [start_date + timedelta(days=i) for i in range(DAYS_OF_DATA)]
    
    data = {
        'date_id': [d.strftime('%Y%m%d') for d in dates],
        'full_date': dates,
        'day_of_week': [d.strftime('%A') for d in dates],
        'day_of_month': [d.day for d in dates],
        'day_of_year': [d.timetuple().tm_yday for d in dates],
        'week_of_year': [d.isocalendar()[1] for d in dates],
        'month': [d.month for d in dates],
        'quarter': [(d.month-1)//3 + 1 for d in dates],
        'year': [d.year for d in dates],
        'is_holiday': [False] * len(dates),
        'holiday_name': [''] * len(dates)
    }
    
    # Add some holidays
    holidays = {
        '2023-01-01': 'New Year\'s Day',
        '2023-12-25': 'Christmas',
        '2023-07-04': 'Independence Day',
        '2023-11-24': 'Thanksgiving'
    }
    
    for date_str, holiday in holidays.items():
        idx = dates.index(datetime.strptime(date_str, '%Y-%m-%d'))
        data['is_holiday'][idx] = True
        data['holiday_name'][idx] = holiday
    
    return pd.DataFrame(data)

def generate_store_data() -> pd.DataFrame:
    """Generate store dimension data."""
    data = {
        'store_id': [f'S{i:03d}' for i in range(1, NUM_STORES + 1)],
        'store_name': [f'Store {i}' for i in range(1, NUM_STORES + 1)],
        'address': [f'{random.randint(1, 9999)} Store St' for _ in range(NUM_STORES)],
        'city': [f'City{i % 50 + 1}' for i in range(NUM_STORES)],
        'state': [f'State{i % 20 + 1}' for i in range(NUM_STORES)],
        'country': ['USA'] * NUM_STORES,
        'postal_code': [f'{random.randint(10000, 99999)}' for _ in range(NUM_STORES)],
        'manager': [f'Manager {i}' for i in range(1, NUM_STORES + 1)],
        'opening_date': pd.date_range(start='2018-01-01', periods=NUM_STORES, freq='D'),
        'store_type': np.random.choice(STORE_TYPES, NUM_STORES),
        'store_size': np.random.uniform(1000, 10000, NUM_STORES).round(2),
        'created_date': pd.date_range(start='2018-01-01', periods=NUM_STORES, freq='D'),
        'modified_date': pd.date_range(start='2018-01-01', periods=NUM_STORES, freq='D')
    }
    return pd.DataFrame(data)

def generate_sales_data(products: pd.DataFrame, customers: pd.DataFrame, 
                       stores: pd.DataFrame, time_dim: pd.DataFrame) -> pd.DataFrame:
    """Generate sales fact data."""
    num_transactions = 100000
    sales_data = []
    
    for _ in range(num_transactions):
        date = random.choice(time_dim['full_date'])
        product = random.choice(products['product_id'])
        customer = random.choice(customers['customer_id'])
        store = random.choice(stores['store_id'])
        
        quantity = random.randint(1, 5)
        unit_price = float(products[products['product_id'] == product]['unit_price'].iloc[0])
        total_amount = quantity * unit_price
        discount_amount = total_amount * random.uniform(0, 0.3)
        net_amount = total_amount - discount_amount
        
        sales_data.append({
            'sale_id': f'T{len(sales_data) + 1:06d}',
            'date_id': date.strftime('%Y%m%d'),
            'product_id': product,
            'customer_id': customer,
            'store_id': store,
            'quantity': quantity,
            'unit_price': unit_price,
            'total_amount': total_amount,
            'discount_amount': discount_amount,
            'net_amount': net_amount,
            'payment_method': random.choice(PAYMENT_METHODS),
            'transaction_time': date + timedelta(hours=random.randint(8, 20),
                                               minutes=random.randint(0, 59))
        })
    
    return pd.DataFrame(sales_data)

def generate_inventory_data(products: pd.DataFrame, stores: pd.DataFrame, 
                          time_dim: pd.DataFrame) -> pd.DataFrame:
    """Generate inventory fact data."""
    inventory_data = []
    
    for store in stores['store_id']:
        for product in products['product_id']:
            for date in time_dim['full_date'][::7]:  # Weekly inventory
                beginning_quantity = random.randint(0, 100)
                units_received = random.randint(0, 50)
                units_sold = random.randint(0, beginning_quantity)
                units_damaged = random.randint(0, 5)
                ending_quantity = beginning_quantity + units_received - units_sold - units_damaged
                
                inventory_data.append({
                    'inventory_id': f'I{len(inventory_data) + 1:08d}',
                    'date_id': date.strftime('%Y%m%d'),
                    'product_id': product,
                    'store_id': store,
                    'beginning_quantity': beginning_quantity,
                    'ending_quantity': ending_quantity,
                    'units_received': units_received,
                    'units_sold': units_sold,
                    'units_damaged': units_damaged,
                    'reorder_point': random.randint(10, 30),
                    'reorder_quantity': random.randint(20, 50)
                })
    
    return pd.DataFrame(inventory_data)

def generate_all_data():
    """Generate all data warehouse tables and save to CSV files."""
    print("Generating dimension tables...")
    products = generate_product_data()
    customers = generate_customer_data()
    time_dim = generate_time_dimension()
    stores = generate_store_data()
    
    print("Generating fact tables...")
    sales = generate_sales_data(products, customers, stores, time_dim)
    inventory = generate_inventory_data(products, stores, time_dim)
    
    # Save to CSV files
    print("Saving data to CSV files...")
    products.to_csv(os.path.join(RAW_DATA_DIR, 'products.csv'), index=False)
    customers.to_csv(os.path.join(RAW_DATA_DIR, 'customers.csv'), index=False)
    time_dim.to_csv(os.path.join(RAW_DATA_DIR, 'time_dimension.csv'), index=False)
    stores.to_csv(os.path.join(RAW_DATA_DIR, 'stores.csv'), index=False)
    sales.to_csv(os.path.join(RAW_DATA_DIR, 'sales.csv'), index=False)
    inventory.to_csv(os.path.join(RAW_DATA_DIR, 'inventory.csv'), index=False)
    
    print("Data generation complete!")

if __name__ == '__main__':
    generate_all_data() 