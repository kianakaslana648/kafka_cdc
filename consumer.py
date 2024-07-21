from confluent_kafka import Consumer
import psycopg2
import json

# Kafka configuration
KAFKA_TOPIC = 'employees_cdc_topic'
KAFKA_BROKER = 'localhost:29092'
GROUP_ID = 'consumer_group'

# PostgreSQL configuration for db2
DB2_CONFIG = {
    'dbname': 'db2',
    'user': 'user2',
    'password': 'password2',
    'host': 'localhost',
    'port': '5433'
}

def process_record(record):
    action = record['action']
    emp_id = record['emp_id']
    first_name = record['first_name']
    last_name = record['last_name']
    dob = record['dob']
    city = record['city']
    
    conn = psycopg2.connect(**DB2_CONFIG)
    cursor = conn.cursor()
    
    if action == 'INSERT':
        cursor.execute("""
            INSERT INTO employees (emp_id, first_name, last_name, dob, city)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (emp_id) DO NOTHING;
        """, (emp_id, first_name, last_name, dob, city))
    
    elif action == 'UPDATE':
        cursor.execute("""
            UPDATE employees
            SET first_name = %s, last_name = %s, dob = %s, city = %s
            WHERE emp_id = %s;
        """, (first_name, last_name, dob, city, emp_id))
    
    elif action == 'DELETE':
        cursor.execute("""
            DELETE FROM employees
            WHERE emp_id = %s;
        """, (emp_id,))
    
    conn.commit()
    cursor.close()
    conn.close()

def main():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    })
    
    consumer.subscribe([KAFKA_TOPIC])
    
    while True:
        msg = consumer.poll(timeout=1.0)  # Adjust timeout as needed
        if msg is not None:
            if msg.error():
                print(f"Consumer error: {msg.error()}")
            else:
                record = json.loads(msg.value().decode('utf-8'))
                process_record(record)


if __name__ == "__main__":
    main()
