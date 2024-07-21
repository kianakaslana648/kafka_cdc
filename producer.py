from confluent_kafka import Producer
import psycopg2
import json
import time
import os

# Kafka configuration
KAFKA_TOPIC = 'employees_cdc_topic'
KAFKA_BROKER = 'localhost:29092'

# PostgreSQL configuration for db1
DB1_CONFIG = {
    'dbname': 'db1',
    'user': 'user1',
    'password': 'password1',
    'host': 'localhost',
    'port': '5432'
}

# Offset file
OFFSET_FILE = 'last_offset.txt'

def get_last_offset():
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE, 'r') as file:
            return int(file.read().strip())
    return 0

def set_last_offset(offset):
    with open(OFFSET_FILE, 'w') as file:
        file.write(str(offset))

def fetch_records(last_offset):
    conn = psycopg2.connect(**DB1_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM employees_cdc WHERE record_id > %s;", (last_offset,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def main():
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    
    last_offset = get_last_offset()
    
    while True:
        records = fetch_records(last_offset)
        
        if records:
            for record in records:
                record_dict = {
                    'emp_id': record[1],
                    'first_name': record[2],
                    'last_name': record[3],
                    'dob': str(record[4]),
                    'city': record[5],
                    'action': record[6]
                }
                producer.produce(KAFKA_TOPIC, value=json.dumps(record_dict))
                last_offset = max(last_offset, record[0])  # Update last_offset based on the highest emp_id
            
            producer.flush()
            set_last_offset(last_offset)
        
        time.sleep(0.5)

if __name__ == "__main__":
    main()
