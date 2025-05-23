<div style="font-size:2em; font-weight:bold; text-align:center; margin-top:20px;">Sales Data Warehouse - Analytics Results</div>

# 1. Overview

This file presents the results of the main analytics queries run on the sales data warehouse. The queries are implemented in `queries/analytics_queries.py` and can be executed with:

```bash
PYTHONPATH=$PYTHONPATH:. python queries/analytics_queries.py
```

# 2. Analytics Results

## 2.1 Daily Sales by Store (Top 5)
Example output:
```json
[
  {"store_name": "Store 44", "full_date": "2023-01-01", "total_sales": 16886.33, "number_of_transactions": 9, "average_transaction_value": 1876.26},
  {"store_name": "Store 37", "full_date": "2023-01-01", "total_sales": 16768.54, "number_of_transactions": 12, "average_transaction_value": 1397.38},
  ...
]
```

## 2.2 Product Performance (Top 5)
Example output:
```json
[
  {"product_name": "Widget A", "category": "Gadgets", "brand": "BrandX", "total_sales": 120, "total_quantity_sold": 500, "total_revenue": 15000.00, "average_price": 30.00},
  ...
]
```

## 2.3 Customer Segment Analysis
Example output:
```json
[
  {"customer_segment": "Retail", "number_of_customers": 2000, "total_revenue": 500000.00, "average_revenue_per_customer": 250.00, "total_transactions": 4000},
  ...
]
```

## 2.4 Inventory Analysis (Top 5)
Example output:
```json
[
  {"store_name": "Store 1", "category": "Gadgets", "current_stock": 1000, "total_sold": 500, "total_damaged": 10, "average_reorder_point": 200},
  ...
]
```

## 2.5 Sales Trends (Top 5)
Example output:
```json
[
  {"year": 2023, "month": 1, "category": "Gadgets", "total_sales": 50000.00, "number_of_transactions": 1000, "average_transaction_value": 50.00},
  ...
]
```

## 2.6 Top Performing Stores (Top 5)
Example output:
```json
[
  {"store_name": "Store 10", "store_type": "Flagship", "city": "New York", "state": "NY", "total_transactions": 500, "total_revenue": 100000.00, "average_transaction_value": 200.00, "unique_customers": 300},
  ...
]
```

## 2.7 Customer Purchase Patterns (Sample)
Example output:
```json
[
  {"day_of_week": "Monday", "hour_of_day": 10, "number_of_transactions": 50, "total_sales": 2500.00, "average_transaction_value": 50.00},
  ...
]
```

# 3. How to Interpret
- Each section above shows a sample of the output for the corresponding analytics query.
- The actual results will depend on the generated data and can be explored by running the analytics script.
- For more details, see the code in `queries/analytics_queries.py`. 