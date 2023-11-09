import os
import pandas as pd
from database import create_conn, execute_query, read_query
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get data file path from environment variables
DATA_FILE_PATH = os.getenv('DATA_FILE_PATH')

# Function to determine the file type
def get_file_type(file_path):
    return file_path.split('.')[-1]

# Function to load data based on file type
def load_data(file_path):
    file_type = get_file_type(file_path)
    if file_type == 'csv':
        return pd.read_csv(file_path)
    elif file_type == 'parquet':
        return pd.read_parquet(file_path)
    elif file_type == 'xlsx' or file_type == 'xls':
        return pd.read_excel(file_path)
    elif file_type == 'txt':
        return pd.read_csv(file_path, delimiter = '\t')
    else:
        print(f"Unsupported file type: {file_type}")
        return None

# Function to process data
def process_data():
    # Load data
    data = load_data(DATA_FILE_PATH)
    if data is None:
        return

    # Create database connection
    conn = create_conn()

    # Process data and save to database
    for _, row in data.iterrows():
        # Write your data processing logic here
        # For example, let's assume we are just inserting the data into a table named 'data_table'
        query = f"INSERT INTO data_table VALUES ({', '.join([str(value) for value in row.values])})"
        execute_query(conn, query)

    # Close database connection
    conn.close()

if __name__ == "__main__":
    process_data()

