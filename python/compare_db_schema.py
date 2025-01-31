from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Function to create a database connection using SQLAlchemy
def get_engine(db_config):
    try:
        # Format for the connection string: 'postgresql://user:password@host:port/database'
        connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        engine = create_engine(connection_string)
        return engine
    except SQLAlchemyError as e:
        print(f"Error in connecting to the database: {e}")
        return None

# Function to fetch the schema of tables from the database using inline SQL
def get_schema(engine, schema_name='public'):
    query = """
    SELECT table_name, column_name, data_type, character_maximum_length, is_nullable
    FROM information_schema.columns
    WHERE table_schema = :schema_name
    ORDER BY table_name, ordinal_position;
    """
    try:
        # Execute the query with the schema name as a parameter
        with engine.connect() as connection:
            result = connection.execute(text(query), {'schema_name': schema_name}).fetchall()
        
        # Organize the results into a dictionary format
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
    except SQLAlchemyError as e:
        print(f"Error while fetching schema: {e}")
        return {}

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
            
            # Compare the column attributes (e.g., nullable, data type, default value)
            if (dev_col['data_type'] != qa_col['data_type'] or
                dev_col['nullable'] != qa_col['nullable'] or
                dev_col['max_length'] != qa_col['max_length']):
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
        'password': 'dev_password',
        'port': 5432  # Default PostgreSQL port
    }
    qa_db_config = {
        'host': 'qa_db_host',
        'database': 'qa_db_name',
        'user': 'qa_user',
        'password': 'qa_password',
        'port': 5432  # Default PostgreSQL port
    }

    # Create engine connections for Dev and QA databases
    dev_engine = get_engine(dev_db_config)
    qa_engine = get_engine(qa_db_config)

    if not dev_engine or not qa_engine:
        print("Failed to connect to one or more databases. Exiting.")
        return
    
    # Fetch schemas with dynamic schema name
    dev_schema = get_schema(dev_engine, schema_name='public')  # Example: 'public' schema
    qa_schema = get_schema(qa_engine, schema_name='public')  # Example: 'public' schema
    
    # Compare schemas
    changes = compare_schemas(dev_schema, qa_schema)
    
    # Print the differences
    print_changes(changes)

if __name__ == "__main__":
    main()
