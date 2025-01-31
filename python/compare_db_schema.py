import pandas as pd
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

# Compare two schemas (Dev vs QA)
def compare_schemas(dev_schema, qa_schema):
    changes = {'added': [], 'removed': [], 'modified': []}
    
    # Compare added or removed tables
    dev_tables = set(dev_schema.keys())
    qa_tables = set(qa_schema.keys())
    
    changes['added'] = list(dev_tables - qa_tables)  # Tables in Dev but not in QA
    changes['removed'] = list(qa_tables - dev_tables)  # Tables in QA but not in Dev

    column_changes = []
    for table_name in dev_tables & qa_tables:  # Only compare tables that exist in both
        dev_columns = {col['column_name']: col for col in dev_schema[table_name]}
        qa_columns = {col['column_name']: col for col in qa_schema[table_name]}

        # Compare added and removed columns
        added_columns = set(dev_columns.keys()) - set(qa_columns.keys())
        removed_columns = set(qa_columns.keys()) - set(dev_columns.keys())

        for col in added_columns:
            column_changes.append({
                'table': table_name,
                'column': col,
                'change': 'added',
                'dev_data_type': dev_columns[col]['data_type'],
                'qa_data_type': None
            })
        
        for col in removed_columns:
            column_changes.append({
                'table': table_name,
                'column': col,
                'change': 'removed',
                'dev_data_type': None,
                'qa_data_type': qa_columns[col]['data_type']
            })

        # Compare modified columns (data type, max length, nullable)
        for column_name in dev_columns & qa_columns:
            dev_col = dev_columns[column_name]
            qa_col = qa_columns[column_name]
            
            if (dev_col['data_type'] != qa_col['data_type'] or
                dev_col['nullable'] != qa_col['nullable'] or
                dev_col['max_length'] != qa_col['max_length']):
                changes['modified'].append({
                    'table': table_name,
                    'column': column_name,
                    'dev_data_type': dev_col['data_type'],
                    'qa_data_type': qa_col['data_type'],
                    'dev_nullable': dev_col['nullable'],
                    'qa_nullable': qa_col['nullable'],
                    'dev_max_length': dev_col['max_length'],
                    'qa_max_length': qa_col['max_length']
                })

    return changes, column_changes

# Generate SQL queries for changes
def generate_sql_queries(dev_schema, qa_schema, dev_schema_name='public', qa_schema_name='public'):
    sql_queries = []

    for table_name, dev_columns in dev_schema.items():
        if table_name in qa_schema:
            qa_columns = qa_schema[table_name]

            # Compare added and removed columns
            dev_column_names = {col['column_name'] for col in dev_columns}
            qa_column_names = {col['column_name'] for col in qa_columns}

            # Added columns (in dev but not in qa)
            added_columns = dev_column_names - qa_column_names
            for column_name in added_columns:
                dev_col = next(col for col in dev_columns if col['column_name'] == column_name)
                data_type = dev_col['data_type']
                max_length = dev_col['max_length']
                nullable = dev_col['nullable']
                add_column_query = f"ALTER TABLE {qa_schema_name}.{table_name} ADD COLUMN {column_name} {data_type} "
                if max_length:
                    add_column_query += f"({max_length}) "
                if nullable.lower() == "no":
                    add_column_query += "NOT NULL;"
                else:
                    add_column_query += ";"
                sql_queries.append({
                    'table': table_name,
                    'column': column_name,
                    'change': 'added',
                    'sql': add_column_query
                })

            # Removed columns (in qa but not in dev)
            removed_columns = qa_column_names - dev_column_names
            for column_name in removed_columns:
                remove_column_query = f"ALTER TABLE {qa_schema_name}.{table_name} DROP COLUMN {column_name};"
                sql_queries.append({
                    'table': table_name,
                    'column': column_name,
                    'change': 'removed',
                    'sql': remove_column_query
                })

            # Modified columns (compare column attributes)
            for column_name in dev_column_names & qa_column_names:
                dev_col = next(col for col in dev_columns if col['column_name'] == column_name)
                qa_col = next(col for col in qa_columns if col['column_name'] == column_name)

                # If there are differences, generate ALTER COLUMN query
                modify_column_query = f"ALTER TABLE {qa_schema_name}.{table_name} ALTER COLUMN {column_name} "
                change_detected = False

                if dev_col['data_type'] != qa_col['data_type']:
                    modify_column_query += f"SET DATA TYPE {dev_col['data_type']} "
                    change_detected = True

                if dev_col['nullable'] != qa_col['nullable']:
                    if dev_col['nullable'] == 'no':
                        modify_column_query += "SET NOT NULL "
                    else:
                        modify_column_query += "DROP NOT NULL "
                    change_detected = True

                if dev_col['max_length'] != qa_col['max_length'] and dev_col['max_length']:
                    modify_column_query += f"SET DATA TYPE {dev_col['data_type']}({dev_col['max_length']}) "
                    change_detected = True

                if change_detected:
                    modify_column_query += ";"
                    sql_queries.append({
                        'table': table_name,
                        'column': column_name,
                        'change': 'modified',
                        'sql': modify_column_query
                    })

    return sql_queries

# Write the result to an Excel file
def write_to_excel(changes, column_changes, sql_queries, file_name="db_comparison_result.xlsx"):
    # Creating dataframes for tables and column changes
    table_changes_df = pd.DataFrame({
        'Change Type': ['Added Tables']*len(changes['added']) + ['Removed Tables']*len(changes['removed']),
        'Table Name': changes['added'] + changes['removed'],
        'Details': ['N/A']*len(changes['added']) + ['N/A']*len(changes['removed'])
    })

    column_changes_df = pd.DataFrame(column_changes)
    
    modified_columns_df = pd.DataFrame(changes['modified'])

    # Add SQL Queries to the column changes DataFrame
    sql_queries_df = pd.DataFrame(sql_queries)
    sql_queries_df['SQL'] = sql_queries_df['sql']

    # Create a Pandas Excel writer using openpyxl engine
    with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
        table_changes_df.to_excel(writer, sheet_name="Table Changes", index=False)
        column_changes_df.to_excel(writer, sheet_name="Column Changes", index=False)
        modified_columns_df.to_excel(writer, sheet_name="Modified Columns", index=False)
        sql_queries_df.to_excel(writer, sheet_name="SQL Queries", index=False)

    print(f"Results written to {file_name}")

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

    # Specify schema names for Dev and QA
    dev_schema_name = 'dev_schema_name'  # Replace with actual schema name for Dev
    qa_schema_name = 'qa_schema_name'    # Replace with actual schema name for QA

    # Create engine connections for Dev and QA databases
    dev_engine = get_engine(dev_db_config)
    qa_engine = get_engine(qa_db_config)

    if not dev_engine or not qa_engine:
        print("Failed to connect to one or more databases. Exiting.")
        return
    
    # Fetch schemas with dynamic schema name
    dev_schema = get_schema(dev_engine, schema_name=dev_schema_name)
    qa_schema = get_schema(qa_engine, schema_name=qa_schema_name)
    
    # Generate SQL queries to replicate changes in QA
    sql_queries = generate_sql_queries(dev_schema, qa_schema, dev_schema_name, qa_schema_name)
    
    # Compare schemas
    changes, column_changes = compare_schemas(dev_schema, qa_schema)
    
    # Write the comparison results to an Excel file
    write_to_excel(changes, column_changes, sql_queries)

if __name__ == "__main__":
    main()
