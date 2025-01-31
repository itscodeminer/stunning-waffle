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

# Generate SQL queries for changes
def generate_sql_queries(dev_schema, qa_schema, schema_name='public'):
    sql_queries = []

    # Generate SQL for added, removed, and modified columns
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
                add_column_query = f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN {column_name} {data_type} "
                if max_length:
                    add_column_query += f"({max_length}) "
                if nullable.lower() == "no":
                    add_column_query += "NOT NULL"
                sql_queries.append({
                    'table': table_name,
                    'column': column_name,
                    'change': 'added',
                    'sql': add_column_query
                })

            # Removed columns (in qa but not in dev)
            removed_columns = qa_column_names - dev_column_names
            for column_name in removed_columns:
                remove_column_query = f"ALTER TABLE {schema_name}.{table_name} DROP COLUMN {column_name};"
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
                if (dev_col['data_type'] != qa_col['data_type'] or
                        dev_col['nullable'] != qa_col['nullable'] or
                        dev_col['max_length'] != qa_col['max_length']):
                    modify_column_query = f"ALTER TABLE {schema_name}.{table_name} ALTER COLUMN {column_name} "
                    if dev_col['data_type'] != qa_col['data_type']:
                        modify_column_query += f"SET DATA TYPE {dev_col['data_type']} "
                    if dev_col['nullable'] != qa_col['nullable']:
                        if dev_col['nullable'] == 'no':
                            modify_column_query += "SET NOT NULL "
                        else:
                            modify_column_query += "DROP NOT NULL "
                    if dev_col['max_length'] != qa_col['max_length'] and dev_col['max_length']:
                        modify_column_query += f"SET DATA TYPE {dev_col['data_type']}({dev_col['max_length']}) "

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

    # Create engine connections for Dev and QA databases
    dev_engine = get_engine(dev_db_config)
    qa_engine = get_engine(qa_db_config)

    if not dev_engine or not qa_engine:
        print("Failed to connect to one or more databases. Exiting.")
        return
    
    # Fetch schemas with dynamic schema name
    dev_schema = get_schema(dev_engine, schema_name='public')  # Example: 'public' schema
    qa_schema = get_schema(qa_engine, schema_name='public')  # Example: 'public' schema
    
    # Generate SQL queries to replicate changes in QA
    sql_queries = generate_sql_queries(dev_schema, qa_schema)
    
    # Compare schemas
    changes, column_changes = compare_schemas(dev_schema, qa_schema)
    
    # Write the comparison results to an Excel file
    write_to_excel(changes, column_changes, sql_queries)

if __name__ == "__main__":
    main()
