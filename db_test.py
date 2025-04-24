import psycopg2

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="training_db",
    user="training_user",
    password="password123",
    host="localhost",
    port="5432"
)

# Create a cursor
cur = conn.cursor()

# Execute a test query
cur.execute("SELECT version();")
print("PostgreSQL version:", cur.fetchone())

# Close connections
cur.close()
conn.close()
