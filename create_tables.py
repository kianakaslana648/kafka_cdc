import psycopg2

# Database connection details
db1_config = {
    'dbname': 'db1',
    'user': 'user1',
    'password': 'password1',
    'host': 'localhost',
    'port': '5432'
}

db2_config = {
    'dbname': 'db2',
    'user': 'user2',
    'password': 'password2',
    'host': 'localhost',
    'port': '5433'
}

# SQL commands to create tables and triggers
create_employees_table = """
CREATE TABLE IF NOT EXISTS employees(
  emp_id SERIAL PRIMARY KEY,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  dob DATE,
  city VARCHAR(100)
);
"""

create_cdc_table = """
CREATE TABLE IF NOT EXISTS employees_cdc(
  record_id SERIAL PRIMARY KEY,
  emp_id INT,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  dob DATE,
  city VARCHAR(100),
  action VARCHAR(10),
  FOREIGN KEY (emp_id) REFERENCES employees (emp_id)
);
"""

create_log_function = """
CREATE OR REPLACE FUNCTION log_employee_changes() RETURNS TRIGGER AS $$
BEGIN
  IF (TG_OP = 'INSERT') THEN
    INSERT INTO employees_cdc (emp_id, first_name, last_name, dob, city, action)
    VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, 'INSERT');
    RETURN NEW;
  ELSIF (TG_OP = 'UPDATE') THEN
    INSERT INTO employees_cdc (emp_id, first_name, last_name, dob, city, action)
    VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, 'UPDATE');
    RETURN NEW;
  ELSIF (TG_OP = 'DELETE') THEN
    INSERT INTO employees_cdc (emp_id, first_name, last_name, dob, city, action)
    VALUES (OLD.emp_id, OLD.first_name, OLD.last_name, OLD.dob, OLD.city, 'DELETE');
    RETURN OLD;
  END IF;
END;
$$ LANGUAGE plpgsql;
"""

create_trigger = """
DROP TRIGGER IF EXISTS employee_changes_trigger ON employees;
CREATE TRIGGER employee_changes_trigger
AFTER INSERT OR UPDATE OR DELETE ON employees
FOR EACH ROW EXECUTE FUNCTION log_employee_changes();
"""

def setup_source_database(config):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**config)
    cursor = conn.cursor()
    
    # Execute SQL commands
    cursor.execute(create_employees_table)
    cursor.execute(create_cdc_table)
    cursor.execute(create_log_function)
    cursor.execute(create_trigger)
    
    # Commit changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()

def setup_target_database(config):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**config)
    cursor = conn.cursor()
    
    # Execute SQL commands
    cursor.execute(create_employees_table)
    
    # Commit changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()


# Set up both databases
setup_source_database(db1_config)
setup_target_database(db2_config)

print("Databases have been set up successfully.")
