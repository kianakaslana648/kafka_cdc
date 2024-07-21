import psycopg2
from psycopg2 import sql

# PostgreSQL configuration for db1
DB1_CONFIG = {
    'dbname': 'db1',
    'user': 'user1',
    'password': 'password1',
    'host': 'localhost',
    'port': '5432'
}

# Sample data to insert
SAMPLE_DATA = [
    ('John', 'Doe', '1980-01-15', 'New York'),
    ('Jane', 'Smith', '1990-07-22', 'Los Angeles'),
    ('Emily', 'Jones', '1985-05-30', 'Chicago'),
    ('Michael', 'Brown', '1979-11-10', 'Houston'),
    ('Laura', 'Wilson', '1992-12-02', 'Phoenix')
]

def insert_sample_data():
    conn = psycopg2.connect(**DB1_CONFIG)
    cursor = conn.cursor()

    # Insert sample data
    insert_query = sql.SQL("""
        INSERT INTO employees (first_name, last_name, dob, city)
        VALUES (%s, %s, %s, %s);
    """)

    for data in SAMPLE_DATA:
        cursor.execute(insert_query, data)

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    insert_sample_data()
