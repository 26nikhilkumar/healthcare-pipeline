import sqlite3

# Path to your SQLite database
db_path = "/config/workspace/case_study/healthcare-pipeline/data/healthcare.db"

# Connect to the database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# List all tables in the database
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("Tables in the database:")
for table in tables:
    print(table[0])

# Display the first few rows of each table
for table in tables:
    print(f"\nFirst 5 rows of table {table[0]}:")
    cursor.execute(f"SELECT * FROM {table[0]} LIMIT 5;")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

# Close the connection
conn.close()
