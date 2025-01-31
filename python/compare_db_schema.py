import psycopg2

# Connect to your Postgres database
def get_connection(db_config):
    return psycopg2.connect(
        host=db_config['host'],
        database=db_config['database'],
        user=db_config['user'],
        password=db_config['password']
    )

# Function to fetch the schema of tables from the database
def get_schema(conn):
    cursor = conn.cursor()
    query = """
    SELECT table_name, column_name, data_type, character_maximum_length, is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'public' -- assuming you are using the 'public' schema
    ORDER BY table_name, ordinal_position;
    """
    cursor.execute(query)
    result = cursor.fetchall()
    schema = {}
    for row in result:
        table_name = row[0]
        column_name = row[1]
        data_type = row[2]
        max_length = row[3]
        nullable = row[4]
        if table_name not in schema:
            schema[table_name] = []
        schema[table_name].append({
            'column_name': column_name,
            'data_type': data_type,
            'max_length': max_length,
            'nullable': nullable
        })
    return schema

# Compare two schemas
def compare_schemas(dev_schema, qa_schema):
    changes = {'added': [], 'removed': [], 'modified': []}
    
    # Compare added or removed tables
    dev_tables = set(dev_schema.keys())
    qa_tables = set(qa_schema.keys())
    
    changes['added'] = list(qa_tables - dev_tables)
    changes['removed'] = list(dev_tables - qa_tables)

    # Compare columns in tables
    for table_name in dev_tables & qa_tables:
        dev_columns = {col['column_name']: col for col in dev_schema[table_name]}
        qa_columns = {col['column_name']: col for col in qa_schema[table_name]}
        
        added_columns = set(qa_columns.keys()) - set(dev_columns.keys())
        removed_columns = set(dev_columns.keys()) - set(qa_columns.keys())

        changes['added'] += [(table_name, col) for col in added_columns]
        changes['removed'] += [(table_name, col) for col in removed_columns]

        # Compare column details for modified columns
        for column_name in dev_columns & qa_columns:
            dev_col = dev_columns[column_name]
            qa_col = qa_columns[column_name]
            
            if dev_col != qa_col:
                changes['modified'].append({
                    'table': table_name,
                    'column': column_name,
                    'dev': dev_col,
                    'qa': qa_col
                })
                
    return changes

# Print the differences
def print_changes(changes):
    print("Added Tables:", changes['added'])
    print("Removed Tables:", changes['removed'])
    print("Added Columns:", changes['added'])
    print("Removed Columns:", changes['removed'])
    print("Modified Columns:", changes['modified'])

# Main function to execute the script
def main():
    dev_db_config = {
        'host': 'dev_db_host',
        'database': 'dev_db_name',
        'user': 'dev_user',
        'password': 'dev_password'
    }
    qa_db_config = {
        'host': 'qa_db_host',
        'database': 'qa_db_name',
        'user': 'qa_user',
        'password': 'qa_password'
    }

    # Connect to Dev and QA databases
    dev_conn = get_connection(dev_db_config)
    qa_conn = get_connection(qa_db_config)
    
    # Fetch schemas
    dev_schema = get_schema(dev_conn)
    qa_schema = get_schema(qa_conn)
    
    # Compare schemas
    changes = compare_schemas(dev_schema, qa_schema)
    
    # Print the differences
    print_changes(changes)
    
    # Close the database connections
    dev_conn.close()
    qa_conn.close()

if __name__ == "__main__":
    main()
