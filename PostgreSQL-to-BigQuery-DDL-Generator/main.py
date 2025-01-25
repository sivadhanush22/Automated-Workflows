import psycopg2

# ------------------ User Configurable Variables ------------------
# PostgreSQL connection details
PG_HOST = 'localhost'
PG_PORT = '5432'
PG_DBNAME = 'postgres'
PG_USER = 'postgres'
PG_PASSWORD = 'postgres'

# PostgreSQL schema name
SCHEMA_NAME = 'public'

# List of tables to process (can include single or multiple table names)
TABLE_LIST = ['test', 'hello']

# Target BigQuery dataset
TARGET_DATASET = 'tables_ods'


# Case transformation styles
# Options: 'camel', 'upper', 'lower', or 'as_is'
TABLE_CASE_STYLE = 'upper'
COLUMN_CASE_STYLE = 'camel'

# Data type mapping for PostgreSQL to BigQuery
PG_TO_BQ_TYPE_MAPPING = {
    'character': 'STRING',
    'character varying': 'STRING',
    'text': 'STRING',
    'varchar': 'STRING',
    'char': 'STRING',
    'integer': 'INT64',
    'numeric': 'NUMERIC',
    'bigint': 'INT64',
    'smallint': 'INT64',
    'double precision': 'FLOAT64',
    'real': 'FLOAT64',
    'boolean': 'BOOL',
    'date': 'DATE',
    'timestamp without time zone': 'DATETIME',
    'timestamp with time zone': 'TIMESTAMP',
    'timestamp': 'TIMESTAMP',
    'array': 'STRING'
}

# ------------------ Utility Functions ------------------

# Function to apply case transformation
def apply_case_transformation(name, case_style):
    """Applies the specified case transformation to a name."""
    if case_style == 'camel':
        return '_'.join(word.capitalize() for word in name.split('_'))
    elif case_style == 'upper':
        return name.upper()
    elif case_style == 'lower':
        return name.lower()
    else:
        return name  # Return as is if no transformation

# Function to connect to PostgreSQL
def connect_to_postgresql(host, port, dbname, user, password):
    """Establishes a connection to the PostgreSQL database."""
    conn = psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password)
    return conn

# Function to fetch table structure from PostgreSQL
def fetch_table_structure_postgresql(conn, schema_name, table_name):
    """Fetches the column names and data types for a given table from PostgreSQL."""
    query = f"""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema = '{schema_name}' 
    AND table_name = '{table_name}';
    """
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

# Function to generate BigQuery DDL
def generate_bigquery_ddl(dataset_name, table_name, columns, type_mapping, table_case_style='as_is', column_case_style='as_is'):
    """Generates BigQuery DDL for a given table."""
    # Apply case transformation for the table name
    table_name = apply_case_transformation(table_name, table_case_style)

    ddl = f"CREATE TABLE `{dataset_name}.{table_name}` (\n"
    column_defs = []
    
    for column_name, data_type in columns:
        # Apply case transformation for each column name
        column_name = apply_case_transformation(column_name, column_case_style)
        mapped_type = type_mapping.get(data_type.lower(), 'STRING')  # Default to STRING if no mapping found
        column_defs.append(f"  {column_name} {mapped_type}")
    
    ddl += ",\n".join(column_defs)
    ddl += "\n);"
    return ddl

# Function to process multiple tables
def generate_ddl_for_tables(conn, dataset_name, schema_name, table_list, type_mapping, table_case_style='as_is', column_case_style='as_is'):
    """Generates BigQuery DDL for a list of tables."""
    for table_name in table_list:
        columns = fetch_table_structure_postgresql(conn, schema_name, table_name)
        ddl = generate_bigquery_ddl(dataset_name, table_name, columns, type_mapping, table_case_style, column_case_style)
        print(f"DDL for table {table_name}:")
        print(ddl)

# Main function to generate DDL scripts
def generate_bigquery_ddl_scripts():
    """Main function to orchestrate the DDL generation process."""
    # Connect to PostgreSQL
    pg_conn = connect_to_postgresql(PG_HOST, PG_PORT, PG_DBNAME, PG_USER, PG_PASSWORD)

    # Generate DDL for the specified tables
    print("\nGenerating DDL for the specified tables:")
    generate_ddl_for_tables(pg_conn, TARGET_DATASET, SCHEMA_NAME, TABLE_LIST, PG_TO_BQ_TYPE_MAPPING, TABLE_CASE_STYLE, COLUMN_CASE_STYLE)

# Run main function
if __name__ == "__main__":
    generate_bigquery_ddl_scripts()
