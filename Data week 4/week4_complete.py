# ============================================================================
# IMPORTS (Add to top of notebook)
# ============================================================================
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# ============================================================================
# PIPELINE 1: TRANSFORM - Add Metadata Function
# ============================================================================
def add_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """
    add ingestion metadata to a pandas dataframe

    Args:
       df (pd.DataFrame): A pandas dataframe
    Return:
       pandas.DataFrame: A pandas dataframe with added metadata
    """
    # Add _ingest_time field with current timestamp
    df['_ingest_time'] = datetime.now()
    
    return df


# Complete the function calls to add _ingest_time to each of the 3 sources
df_users = add_metadata(df_users)
df_orders = add_metadata(df_orders)
df_stores = add_metadata(df_stores)


# ============================================================================
# PIPELINE 1: LOAD - Setup PostgreSQL and Create Functions
# ============================================================================

# Start PostgreSQL service and create database
# Note: Run these commands in terminal or use subprocess
"""
sudo service postgresql start
sudo -u postgres psql -c "CREATE USER student WITH PASSWORD 'student_password';"
sudo -u postgres psql -c "CREATE DATABASE pipeline_db OWNER student;"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE pipeline_db TO student;"
"""

# Create engine to connect to database
engine = create_engine('postgresql://student:student_password@localhost:5432/pipeline_db')


def check_schema(df: pd.DataFrame, expected_schema: dict) -> bool:
    """
    check whether schema matches expected schema

    Args:
       df (pd.DataFrame): A pandas dataframe
       expected_schema (dict): Expected schema for dataframe
    Return:
       bool: True/False whether schema matches
    """
    # Get actual schema from dataframe
    actual_schema = df.dtypes.astype(str).to_dict()
    
    # Check if all expected columns exist and have correct types
    for col, dtype in expected_schema.items():
        if col not in actual_schema:
            print(f"Error: Column '{col}' missing from dataframe")
            return False
        if actual_schema[col] != dtype:
            print(f"Error: Column '{col}' has type '{actual_schema[col]}' but expected '{dtype}'")
            return False
    
    return True


def write_to_postgres(engine, df, expected_schema, table_name):
    """
    load data to postgres database if schema matches expected schema

    Args:
       engine: Engine to connect to postgres database
       df (pd.DataFrame): A pandas dataframe
       expected_schema (dict): Expected schema for dataframe
       table_name (str): Name of table to be loaded
    Return:
       None
    """
    # Check if schema matches
    if check_schema(df, expected_schema):
        print("Schema matches.")
        # Write dataframe to postgres
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"DataFrame written to table '{table_name}'.")
    else:
        print(f"Error: Schema mismatch for table '{table_name}'. Data not loaded.")


# ============================================================================
# LOAD USERS TABLE
# ============================================================================

# Define expected schema
expected_schema_users = {
    'user_id': 'int64',
    'name': 'string',
    'gender': 'string',
    'age': 'int64',
    'address': 'string',
    'date_joined': 'datetime64[s]',
    '_ingest_time': 'datetime64[s]',
}

# Cast data types
df_users = df_users.astype({
    'user_id': 'int64',
    'name': 'string',
    'gender': 'string',
    'age': 'int64',
    'address': 'string'
})
df_users['date_joined'] = pd.to_datetime(df_users['date_joined']).astype('datetime64[s]')
df_users['_ingest_time'] = pd.to_datetime(df_users['_ingest_time']).astype('datetime64[s]')

# Get today's date for table naming
today = datetime.now().strftime('%Y%m%d')

# Complete the function call to load users_YYYYMMDD
write_to_postgres(engine, df_users, expected_schema_users, f'users_{today}')


# ============================================================================
# LOAD ORDERS TABLE
# ============================================================================

# Define expected schema
expected_schema_orders = {
    'order_no': 'int64',
    'user_id': 'int64',
    'product_list': 'string',
    'date_purchased': 'datetime64[s]',
    'store_id': 'int64',
    '_ingest_time': 'datetime64[s]',
}

# Cast data types
df_orders = df_orders.astype({
    'order_no': 'int64',
    'user_id': 'int64',
    'product_list': 'string',
    'store_id': 'int64'
})
df_orders['date_purchased'] = pd.to_datetime(df_orders['date_purchased']).astype('datetime64[s]')
df_orders['_ingest_time'] = pd.to_datetime(df_orders['_ingest_time']).astype('datetime64[s]')

# Complete the function call to load orders_YYYYMMDD
write_to_postgres(engine, df_orders, expected_schema_orders, f'orders_{today}')


# ============================================================================
# LOAD STORES TABLE
# ============================================================================

# Define expected schema
expected_schema_stores = {
    'store_id': 'int64',
    'name': 'string',
    'address': 'string',
    'hours': 'string',
    '_ingest_time': 'datetime64[s]',
}

# Cast data types
df_stores = df_stores.astype({
    'store_id': 'int64',
    'name': 'string',
    'address': 'string',
    'hours': 'string'
})
df_stores['_ingest_time'] = pd.to_datetime(df_stores['_ingest_time']).astype('datetime64[s]')

# Complete the function call to load stores_YYYYMMDD
write_to_postgres(engine, df_stores, expected_schema_stores, f'stores_{today}')


# ============================================================================
# PIPELINE 1: RUN END-TO-END
# ============================================================================

# Extract data
df_users = gcs_extract('bdt-beam', 'users_v.csv')
df_orders = s3_extract('dev-training-analytics', 'df_orders.parquet')
df_stores = tinydb_extract('stores.json')

# Transform
df_users = add_metadata(df_users)
df_orders = add_metadata(df_orders)
df_stores = add_metadata(df_stores)

# Load
today = datetime.now().strftime('%Y%m%d')

# Cast and load users
df_users = df_users.astype({'user_id': 'int64', 'name': 'string', 'gender': 'string', 'age': 'int64', 'address': 'string'})
df_users['date_joined'] = pd.to_datetime(df_users['date_joined']).astype('datetime64[s]')
df_users['_ingest_time'] = pd.to_datetime(df_users['_ingest_time']).astype('datetime64[s]')
write_to_postgres(engine, df_users, expected_schema_users, f'users_{today}')

# Cast and load orders
df_orders = df_orders.astype({'order_no': 'int64', 'user_id': 'int64', 'product_list': 'string', 'store_id': 'int64'})
df_orders['date_purchased'] = pd.to_datetime(df_orders['date_purchased']).astype('datetime64[s]')
df_orders['_ingest_time'] = pd.to_datetime(df_orders['_ingest_time']).astype('datetime64[s]')
write_to_postgres(engine, df_orders, expected_schema_orders, f'orders_{today}')

# Cast and load stores
df_stores = df_stores.astype({'store_id': 'int64', 'name': 'string', 'address': 'string', 'hours': 'string'})
df_stores['_ingest_time'] = pd.to_datetime(df_stores['_ingest_time']).astype('datetime64[s]')
write_to_postgres(engine, df_stores, expected_schema_stores, f'stores_{today}')


# ============================================================================
# PIPELINE 2: EXTRACT - Read from PostgreSQL
# ============================================================================

def read_from_postgres(engine, table_name) -> pd.DataFrame:
    """
    read data from postgres database into a pandas dataframe

    Args:
       engine: Engine to connect to postgres database
       table_name (str): Name of table to be extracted
    Return:
       A pandas dataframe
    """
    # Read data from postgres table into dataframe
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)
    
    return df


# Complete the function call to load the data
today = datetime.now().strftime('%Y%m%d')
df_users = read_from_postgres(engine, f'users_{today}')
df_orders = read_from_postgres(engine, f'orders_{today}')
df_stores = read_from_postgres(engine, f'stores_{today}')


# ============================================================================
# PIPELINE 2: LOAD - Modified Load Function
# ============================================================================

def load(transformed: pd.DataFrame, database_name: str, table_name: str) -> None:
    """
    Write the dataframe to a local sqlite db with specified schema

    Args:
      transformed: a dataframe with columns ['date_purchased','province','store_id', 'gender', 'loyalty', 'total_purchases']
      database_name: name for the local SQLite DB you are creating
      table_name: name of the table you are loading the data to
    Return:
      None
    """
    try:
        # Create a local SQLite database
        db_path = database_name
        conn = sqlite3.connect(db_path)

        # Convert your datetime to ISO 8601 format (YYYY-MM-DD HH:MM:SS) for loading to SQLite
        transformed['date_purchased'] = transformed['date_purchased'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Define schema and create table (if needed)
        schema = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            date_purchased TEXT NOT NULL,
            province TEXT NOT NULL,
            store_id INTEGER NOT NULL,
            gender TEXT NOT NULL,
            loyalty TEXT NOT NULL,
            total_purchases INTEGER NOT NULL
        )
        '''
        conn.execute(schema)

        # Write the DataFrame to the SQLite database with table sharding
        # Use if_exists='replace' to ensure idempotency for daily sharded tables
        transformed.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"DataFrame successfully written to '{table_name}' in database '{database_name}'.")
        
        # Close connection
        conn.close()
        
    except Exception as e:
        # Handle any errors that might occur during writing
        print(f"Error writing DataFrame to database: {e}")


# Execute your load() function to load your data into the SQLite database
today = datetime.now().strftime('%Y%m%d')
load(df_transform, 'local_sqlite_database.db', f'sales_aggregated_{today}')


# ============================================================================
# PIPELINE 2: RUN END-TO-END
# ============================================================================

# Extract data
today = datetime.now().strftime('%Y%m%d')
df_users = read_from_postgres(engine, f'users_{today}')
df_orders = read_from_postgres(engine, f'orders_{today}')
df_stores = read_from_postgres(engine, f'stores_{today}')

# Transform
df_transform = transform(df_users, df_orders, df_stores)

# Load
load(df_transform, 'local_sqlite_database.db', f'sales_aggregated_{today}')
