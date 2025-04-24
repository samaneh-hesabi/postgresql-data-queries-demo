from data_analysis import create_connection, import_csv_to_postgres, execute_query

# Example 1: Import CSV data into PostgreSQL
print("\nExample 1: Importing Titanic dataset")
import_csv_to_postgres('data/titanic.csv', 'titanic')

# Example 2: Basic SELECT query
print("\nExample 2: View first 5 passengers")
query = """
SELECT "Name", "Age", "Sex", "Pclass"
FROM titanic
LIMIT 5
"""
execute_query(query, "First 5 passengers")

# Example 3: Filtering data
print("\nExample 3: Find all first-class passengers")
query = """
SELECT "Name", "Age", "Sex", "Pclass"
FROM titanic
WHERE "Pclass" = 1
LIMIT 5
"""
execute_query(query, "First-class passengers")

# Example 4: Aggregation
print("\nExample 4: Average age by passenger class")
query = """
SELECT 
    "Pclass",
    COUNT(*) as total_passengers,
    ROUND(AVG("Age"), 2) as average_age
FROM titanic
GROUP BY "Pclass"
ORDER BY "Pclass"
"""
execute_query(query, "Statistics by passenger class")

# Example 5: Complex query with multiple conditions
print("\nExample 5: Find young survivors")
query = """
SELECT 
    "Name",
    "Age",
    "Sex",
    "Pclass",
    "Survived"
FROM titanic
WHERE "Age" < 18 
    AND "Survived" = 1
    AND "Sex" = 'female'
ORDER BY "Age"
LIMIT 5
"""
execute_query(query, "Young female survivors")

# Example 6: Using the connection directly
print("\nExample 6: Using direct connection")
conn = create_connection()
if conn:
    try:
        cur = conn.cursor()
        # Create a new table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                value INTEGER
            )
        """)
        
        # Insert some data
        cur.execute("""
            INSERT INTO test_table (name, value)
            VALUES 
                ('Test 1', 100),
                ('Test 2', 200),
                ('Test 3', 300)
        """)
        
        # Commit the transaction
        conn.commit()
        print("Successfully created and populated test_table")
        
        # Query the new table
        cur.execute("SELECT * FROM test_table")
        results = cur.fetchall()
        print("\nContents of test_table:")
        for row in results:
            print(row)
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close() 