from data_analysis import create_connection, import_csv_to_postgres, execute_query, DB_PARAMS
import pandas as pd
from sqlalchemy import create_engine

# Example 1: Advanced Filtering with Multiple Conditions
print("\nExample 1: Advanced Filtering")
query = """
SELECT 
    "Name",
    "Age",
    "Sex",
    "Pclass",
    "Fare",
    "Survived"
FROM titanic
WHERE 
    ("Pclass" = 1 OR "Pclass" = 2)  -- First or second class
    AND "Age" BETWEEN 20 AND 40     -- Age range
    AND "Fare" > 50                 -- Expensive tickets
    AND "Survived" = 1              -- Survived
ORDER BY "Fare" DESC
LIMIT 10
"""
execute_query(query, "Survived adults in first/second class with expensive tickets")

# Example 2: Complex Aggregation with Multiple Groups
print("\nExample 2: Complex Aggregation")
query = """
SELECT 
    "Pclass",
    "Sex",
    COUNT(*) as total_passengers,
    ROUND(CAST(AVG("Age") AS numeric), 2) as average_age,
    ROUND(CAST(AVG("Fare") AS numeric), 2) as average_fare,
    SUM(CASE WHEN "Survived" = 1 THEN 1 ELSE 0 END) as survivors,
    ROUND(CAST(SUM(CASE WHEN "Survived" = 1 THEN 1 ELSE 0 END) AS numeric) / COUNT(*) * 100, 2) as survival_rate
FROM titanic
GROUP BY "Pclass", "Sex"
ORDER BY "Pclass", "Sex"
"""
execute_query(query, "Detailed statistics by class and gender")

# Example 3: Subqueries and Joins
print("\nExample 3: Subqueries and Joins")
query = """
WITH class_stats AS (
    SELECT 
        "Pclass",
        ROUND(CAST(AVG("Fare") AS numeric), 2) as avg_fare
    FROM titanic
    GROUP BY "Pclass"
)
SELECT 
    t."Name",
    t."Pclass",
    t."Fare",
    cs.avg_fare,
    ROUND(CAST(t."Fare" - cs.avg_fare AS numeric), 2) as fare_difference
FROM titanic t
JOIN class_stats cs ON t."Pclass" = cs."Pclass"
WHERE ABS(t."Fare" - cs.avg_fare) > 50
ORDER BY fare_difference DESC
LIMIT 10
"""
execute_query(query, "Passengers who paid significantly more than class average")

# Example 4: Window Functions
print("\nExample 4: Window Functions")
query = """
SELECT 
    "Name",
    "Age",
    "Pclass",
    "Fare",
    RANK() OVER (PARTITION BY "Pclass" ORDER BY "Fare" DESC) as fare_rank_in_class,
    ROUND(CAST(AVG("Fare") OVER (PARTITION BY "Pclass") AS numeric), 2) as class_avg_fare
FROM titanic
WHERE "Pclass" IN (1, 2, 3)
ORDER BY "Pclass", fare_rank_in_class
LIMIT 15
"""
execute_query(query, "Passenger rankings by fare within each class")

# Example 5: Data Modification and Transactions
print("\nExample 5: Data Modification")
conn = create_connection()
if conn:
    try:
        cur = conn.cursor()
        
        # Create a temporary table for analysis
        cur.execute("""
            CREATE TEMP TABLE temp_analysis AS
            SELECT 
                "Pclass",
                "Sex",
                COUNT(*) as count,
                AVG("Age") as avg_age
            FROM titanic
            GROUP BY "Pclass", "Sex"
        """)
        
        # Update the temporary table
        cur.execute("""
            UPDATE temp_analysis
            SET avg_age = ROUND(CAST(avg_age AS numeric), 2)
        """)
        
        # Query the results
        cur.execute("SELECT * FROM temp_analysis ORDER BY \"Pclass\", \"Sex\"")
        results = cur.fetchall()
        print("\nAnalysis Results:")
        for row in results:
            print(row)
            
        # Clean up
        cur.execute("DROP TABLE temp_analysis")
        conn.commit()
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

# Example 6: Advanced Data Import with Transformation
print("\nExample 6: Advanced Data Import")
def advanced_import():
    try:
        # Read the CSV with specific options
        df = pd.read_csv('data/titanic.csv')
        
        # Add derived columns
        df['age_group'] = pd.cut(df['Age'], 
                                bins=[0, 18, 30, 50, 100],
                                labels=['child', 'young', 'adult', 'senior'])
        
        df['fare_per_person'] = df['Fare'] / (df['SibSp'] + df['Parch'] + 1)
        
        # Create SQLAlchemy engine
        engine = create_engine(f'postgresql://{DB_PARAMS["user"]}:{DB_PARAMS["password"]}@{DB_PARAMS["host"]}:{DB_PARAMS["port"]}/{DB_PARAMS["dbname"]}')
        
        # Import to PostgreSQL with specific options
        df.to_sql('titanic_enhanced', 
                 engine, 
                 if_exists='replace',
                 index=False,
                 method='multi',
                 chunksize=1000)
        
        print("Successfully imported enhanced Titanic dataset")
        
        # Verify the import
        query = """
        SELECT 
            age_group,
            COUNT(*) as count,
            ROUND(CAST(AVG(fare_per_person) AS numeric), 2) as avg_fare_per_person
        FROM titanic_enhanced
        GROUP BY age_group
        ORDER BY age_group
        """
        execute_query(query, "Enhanced dataset statistics")
        
    except Exception as e:
        print(f"Error in advanced import: {e}")

# Run the advanced import
advanced_import() 