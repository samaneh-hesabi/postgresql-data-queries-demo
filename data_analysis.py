import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# Database connection parameters
DB_PARAMS = {
    'dbname': 'training_db',
    'user': 'training_user',
    'password': 'password123',
    'host': 'localhost',
    'port': '5432'
}

def create_connection():
    """Create a connection to the PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def import_csv_to_postgres(csv_file, table_name):
    """Import a CSV file into PostgreSQL using pandas and sqlalchemy"""
    try:
        # Create SQLAlchemy engine
        engine = create_engine(f'postgresql://{DB_PARAMS["user"]}:{DB_PARAMS["password"]}@{DB_PARAMS["host"]}:{DB_PARAMS["port"]}/{DB_PARAMS["dbname"]}')
        
        # Read CSV file
        df = pd.read_csv(csv_file)
        
        # Import to PostgreSQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Successfully imported {csv_file} to table {table_name}")
        
    except Exception as e:
        print(f"Error importing data: {e}")

def execute_query(query, description=""):
    """Execute a SQL query and return the results"""
    print(f"\n--- {description} ---")
    print("Query:")
    print(query)
    
    conn = create_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(query)
            results = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            cur.close()
            conn.close()
            df = pd.DataFrame(results, columns=columns)
            print("\nResults:")
            print(df)
            return df
        except Exception as e:
            print(f"Error executing query: {e}")
            return None
    return None

# Example usage
if __name__ == "__main__":
    # Import the Titanic dataset
    import_csv_to_postgres('data/titanic.csv', 'titanic')
    
    # 1. Basic SELECT
    execute_query(
        "SELECT * FROM titanic LIMIT 5",
        "1. Basic SELECT - Show first 5 rows"
    )
    
    # 2. SELECT specific columns
    execute_query(
        """
        SELECT "Name", "Age", "Sex"
        FROM titanic
        LIMIT 5
        """,
        "2. SELECT specific columns"
    )
    
    # 3. WHERE clause
    execute_query(
        """
        SELECT "Name", "Age", "Sex"
        FROM titanic
        WHERE "Age" < 10
        LIMIT 5
        """,
        "3. WHERE clause - Find young passengers (under 10)"
    )
    
    # 4. ORDER BY
    execute_query(
        """
        SELECT "Name", "Age", "Fare"
        FROM titanic
        ORDER BY "Fare" DESC
        LIMIT 5
        """,
        "4. ORDER BY - Most expensive tickets"
    )
    
    # 5. GROUP BY with COUNT
    execute_query(
        """
        SELECT "Sex", COUNT(*) as passenger_count
        FROM titanic
        GROUP BY "Sex"
        """,
        "5. GROUP BY - Count passengers by gender"
    )
    
    # 6. Multiple conditions
    execute_query(
        """
        SELECT "Name", "Age", "Sex", "Pclass", "Survived"
        FROM titanic
        WHERE "Age" < 18 
        AND "Sex" = 'female' 
        AND "Pclass" = 1
        """,
        "6. Multiple conditions - Young, female, first-class passengers"
    )
    
    # 7. Aggregate functions
    execute_query(
        """
        SELECT 
            "Pclass",
            COUNT(*) as total_passengers,
            ROUND(CAST(AVG("Age") AS numeric), 2) as avg_age,
            ROUND(CAST(AVG("Fare") AS numeric), 2) as avg_fare
        FROM titanic
        GROUP BY "Pclass"
        ORDER BY "Pclass"
        """,
        "7. Aggregate functions - Statistics by class"
    )
    
    # 8. HAVING clause
    execute_query(
        """
        SELECT 
            "Pclass",
            COUNT(*) as survivor_count,
            ROUND(CAST(AVG("Age") AS numeric), 2) as avg_age
        FROM titanic
        WHERE "Survived" = 1
        GROUP BY "Pclass"
        HAVING COUNT(*) > 100
        ORDER BY survivor_count DESC
        """,
        "8. HAVING clause - Classes with more than 100 survivors"
    )
    
    # Test connection
    conn = create_connection()
    if conn:
        print("Database connection successful!")
        conn.close() 