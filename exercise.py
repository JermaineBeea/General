# Data Eng: Week 1 Practical - Complete Solution

# Setup your python environment
import pandas as pd

# Download the data files
import os
import urllib.request

def download_file(url, filename):
    """Download file if it doesn't exist"""
    if not os.path.exists(filename):
        print(f"Downloading {filename}...")
        urllib.request.urlretrieve(url, filename)
        print(f"{filename} downloaded successfully!")
    else:
        print(f"{filename} already exists.")

# Download orders and users files
download_file('https://storage.googleapis.com/bdt-beam/orders_v_2022.csv', 'orders.csv')
download_file('https://storage.googleapis.com/bdt-beam/users_v.csv', 'users.csv')

# EXTRACT FUNCTION
def extract(csv_filename: str, delimiter: str=',') -> pd.DataFrame:
    """
    Extract data from a file and structure it into a pandas dataframe
    
    Args:
        csv_filename (str): name of csv file
        delimiter (str): default ','
    
    Return:
        pandas.DataFrame: A pandas dataframe with header as defined by the csv file
    """
    try:
        # Read the CSV file, skipping rows that start with '#' (comments)
        df = pd.read_csv(csv_filename, delimiter=delimiter, comment='#')
        print(f"Successfully extracted {len(df)} rows from {csv_filename}")
        return df
    except FileNotFoundError:
        print(f"Error: File '{csv_filename}' not found.")
        return None
    except Exception as e:
        print(f"Error reading file: {e}")
        return None

# Extract users data
print("\n=== Extracting Users Data ===")
df_users = extract('users.csv')
if df_users is not None:
    print(f"Users DataFrame shape: {df_users.shape}")
    print("\nFirst 3 rows of users:")
    print(df_users.head(3))

# Extract orders data
print("\n=== Extracting Orders Data ===")
df_orders = extract('orders.csv')
if df_orders is not None:
    print(f"Orders DataFrame shape: {df_orders.shape}")
    print("\nFirst 3 rows of orders:")
    print(df_orders.head(3))

# TRANSFORM FUNCTION
def transform(users: pd.DataFrame, orders: pd.DataFrame) -> pd.DataFrame:
    """
    Take two dataframes and produce a dataset per requirements
    
    Args:
        users: a dataframe of user information
        orders: a dataframe of user orders
    
    Return:
        pandas.DataFrame: A pandas dataframe with columns 
        ['date_purchased', 'gender', 'total_purchases']
    """
    try:
        # Join the dataframes on user_id
        merged_df = orders.merge(users, on='user_id', how='inner')
        print(f"Merged dataframe shape: {merged_df.shape}")
        
        # Group by date_purchased and gender, count the number of orders
        result = merged_df.groupby(['date_purchased', 'gender']).size().reset_index(name='total_purchases')
        
        # Sort by date and gender for better readability
        result = result.sort_values(['date_purchased', 'gender'])
        
        print(f"Transformed dataframe shape: {result.shape}")
        return result
    except Exception as e:
        print(f"Error during transformation: {e}")
        return None

# Transform the data
print("\n=== Transforming Data ===")
df_transformed = transform(df_users, df_orders)
if df_transformed is not None:
    print("\nFirst 10 rows of transformed data:")
    print(df_transformed.head(10))

# LOAD FUNCTION
def load(transformed: pd.DataFrame, filename: str, delimiter: str=',') -> None:
    """
    Write the dataframe to a csv with specified delimiter
    
    Args:
        transformed: a dataframe with columns ['date_purchased','gender','total_purchases']
        filename: a filename for the output CSV
        delimiter: an optional delimiter signifying column boundaries in the CSV file
    
    Return:
        None
    """
    try:
        transformed.to_csv(filename, sep=delimiter, index=False)
        print(f"Successfully wrote {len(transformed)} rows to {filename}")
    except Exception as e:
        print(f"Error writing file: {e}")

# PIPELINE - Run end to end
print("\n=== Running Complete ETL Pipeline ===")
try:
    # Extract
    print("\nStep 1: Extract")
    users_df = extract('users.csv')
    orders_df = extract('orders.csv')
    
    if users_df is not None and orders_df is not None:
        # Transform
        print("\nStep 2: Transform")
        transformed_df = transform(users_df, orders_df)
        
        if transformed_df is not None:
            # Load
            print("\nStep 3: Load")
            load(transformed_df, 'output.csv')
            
            print("\n=== Pipeline Complete! ===")
            print("\nFinal output preview:")
            print(transformed_df.head(10))
            
            # Verify the output file
            if os.path.exists('output.csv'):
                print("\n✓ Output file 'output.csv' created successfully!")
        else:
            print("Transform step failed.")
    else:
        print("Extract step failed.")
except Exception as e:
    print(f"Pipeline error: {e}")

# Display statistics
print("\n=== Summary Statistics ===")
if df_transformed is not None:
    print(f"Total rows in output: {len(df_transformed)}")
    print(f"Date range: {df_transformed['date_purchased'].min()} to {df_transformed['date_purchased'].max()}")
    print("\nTotal purchases by gender:")
    print(df_transformed.groupby('gender')['total_purchases'].sum())