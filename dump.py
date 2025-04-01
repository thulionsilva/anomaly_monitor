import pandas as pd
import psycopg2
from sqlalchemy import except_
from datetime import timedelta

# Create a sample dataframe (replace with your own)

df = pd.read_csv("C:\\Users\\thuli\\Downloads\\chlorum_dump.csv")
df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms') - timedelta(hours=3)

# Database connection parameters
conn_params = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 1234,
    'port': 5433
}

# Connect to PostgreSQL
conn = psycopg2.connect(**conn_params)
cursor = conn.cursor()

# Insert data row by row using cursor
table_name = 'chlorum_dump'
try:
    for index, row in df.iterrows():
        # Create the INSERT query
        query = f"INSERT INTO public.chlorum_dump (Variable, Value, \"Timestamp\") VALUES (%s, %s, %s)"
        values = (row['Variable'], row['Value'], row['Timestamp'])
        
        # Execute the query
        cursor.execute(query, values)
    print("Sucess")
except Exception as e:
    conn.rollback()
    print("Error while inserting data!")
    print(e)

finally:
    # Commit and close
    conn.commit()
    cursor.close()
    conn.close()

