<div style="font-size:2.5em; font-weight:bold; text-align:center; margin-top:20px;">Sales Data Warehouse - Data Structure</div>

# 1. Directory Structure

- `raw/`: Contains original, unprocessed data files (CSV format)
- `processed/`: (Reserved for future use) Contains cleaned and transformed data ready for loading

# 1.1 Data Sources

The sales data warehouse will include the following data sources:

## 1.1.1 Dimension Tables

1. **Product Dimension**
   - Product ID
   - Product Name
   - Category
   - Subcategory
   - Brand
   - Unit Price
   - Cost
   - Created Date
   - Modified Date

2. **Customer Dimension**
   - Customer ID
   - First Name
   - Last Name
   - Email
   - Phone
   - Address
   - City
   - State
   - Country
   - Postal Code
   - Customer Segment
   - Created Date
   - Modified Date

3. **Time Dimension**
   - Date ID
   - Full Date
   - Day of Week
   - Day of Month
   - Day of Year
   - Week of Year
   - Month
   - Quarter
   - Year
   - Is Holiday
   - Holiday Name

4. **Store Dimension**
   - Store ID
   - Store Name
   - Address
   - City
   - State
   - Country
   - Postal Code
   - Manager
   - Opening Date
   - Store Type
   - Store Size
   - Created Date
   - Modified Date

## 1.1.2 Fact Tables

1. **Sales Fact**
   - Sale ID
   - Date ID (FK)
   - Product ID (FK)
   - Customer ID (FK)
   - Store ID (FK)
   - Quantity
   - Unit Price
   - Total Amount
   - Discount Amount
   - Net Amount
   - Payment Method
   - Transaction Time

2. **Inventory Fact**
   - Inventory ID
   - Date ID (FK)
   - Product ID (FK)
   - Store ID (FK)
   - Beginning Quantity
   - Ending Quantity
   - Units Received
   - Units Sold
   - Units Damaged
   - Reorder Point
   - Reorder Quantity

# 1.2 Data Formats

- Raw data will be stored in CSV format
- Processed data will be stored in both CSV and Parquet formats
- All dates will be in ISO format (YYYY-MM-DD)
- All monetary values will be in USD with 2 decimal places
- All IDs will be unique and follow a consistent format

# 1.3 Data Quality Rules

1. **Completeness**
   - No null values in primary keys
   - Required fields must be populated
   - Optional fields can be null

2. **Consistency**
   - All dates must be valid
   - Monetary values must be positive
   - Quantities must be non-negative integers
   - IDs must be unique within their domain

3. **Accuracy**
   - Prices must match product catalog
   - Totals must be calculated correctly
   - Dates must be within valid ranges

4. **Timeliness**
   - Data will be refreshed daily
   - Historical data will be preserved
   - Changes will be tracked using slowly changing dimensions

# 1.4 Data Analysis Results

The project includes sample analytics queries to analyze the sales data. The results of these queries can be found in the `queries/analytics_queries.py` file. To run the analytics queries, use the following command:

```bash
PYTHONPATH=$PYTHONPATH:. python queries/analytics_queries.py
```

This will display the results of the analytics queries in a clean, readable format. 