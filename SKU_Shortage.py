import pandas as pd
import mysql.connector
from mysql.connector import Error
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import os
import time
import re
import logging
import sys
from pathlib import Path
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime  # 🔹 Import for timestamp
from google.oauth2 import service_account
import pytz
import traceback
from selenium.webdriver.common.keys import Keys  
import calendar
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
log = logging.getLogger()

# Google Sheet details for both sheets
sheet_id_1 = "1oGev0ICLnkDxEPs-wNKE-GMraHQX57ELODgReTINPJ4"
sheet_name_1 = "Sheet1"

sheet_id_2 = "1kK6b4IPJz5y6ESWqvxGGk6KUsiDXOOjyaK4IRo0UzxU"
sheet_name_2 = "Sheet1"

# New Sheet (Third Sheet) details
sheet_id_3 = "1-OwCIObhsTk25-t0AcyyI92Vn7HihRTDjnBdagp5iAA"
sheet_name_3 = "Sheet1"  # Update if the sheet name is different




# Construct the CSV export URLs
csv_url_1 = f"https://docs.google.com/spreadsheets/d/{sheet_id_1}/gviz/tq?tqx=out:csv&sheet={sheet_name_1}"
csv_url_2 = f"https://docs.google.com/spreadsheets/d/{sheet_id_2}/gviz/tq?tqx=out:csv&sheet={sheet_name_2}"
csv_url_3 = f"https://docs.google.com/spreadsheets/d/{sheet_id_3}/gviz/tq?tqx=out:csv&sheet={sheet_name_3}"
# csv_url_4 = pd.read_csv("stock.csv")

# Function to load data
def load_data(csv_url):
    try:
        # Load CSV data
        df = pd.read_csv(csv_url, low_memory=False)
        
        # Clean the column names
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Replace NaN values with an empty string or NULL
        df = df.fillna('')  # You can replace '' with 'NULL' if you prefer NULL for missing values
        
        # Rename first column to 'Issued_On'
        if df.columns[0] != 'Issued_On':
            df.rename(columns={df.columns[0]: 'Issued_On'}, inplace=True)
        
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None
    
    
# Function to clean column names (remove spaces, special characters, etc.)
def clean_column_name(col_name):
    col_name = col_name.strip()  # Remove leading/trailing spaces
    col_name = re.sub(r'[^A-Za-z0-9_]+', '', col_name)  # Replace non-alphanumeric characters with ''
    col_name = col_name.replace(' ', '_')  # Replace spaces with underscores
    col_name = col_name.replace('-', '_')  # Replace hyphens with underscores
    return col_name

# Load the data from both sheets
df_1 = load_data(csv_url_1)
df_2 = load_data(csv_url_2)
df_3 = load_data(csv_url_3)


# Show the first 5 rows of both dataframes
print("Data from Sheet 1 (first 5 rows):")
print(df_1.head())

print("\nData from Sheet 2 (first 5 rows):")
print(df_2.head())

print("\nData from Sheet 2 (first 5 rows):")
print(df_3.head())


# MySQL Database connection details
db_config = {
    'host': 'localhost', 
    'database': 'rm_shortage', 
    'user': 'root', 
    'password': '',
    'port': 3306
}

# Function to create MySQL connection
def create_connection():
    try:
        conn = mysql.connector.connect(**db_config)
        if conn.is_connected():
            print("Connected to MySQL database")
            return conn
    except Error as e:
        print(f"Error: {e}")
        return None

# Function to drop and recreate the table, then insert data
def insert_data_to_db(df, table_name):
    try:
        conn = create_connection()
        if conn:
            cursor = conn.cursor()

            # Drop the table if it exists
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"Table '{table_name}' dropped successfully (if it existed).")

            # Mapping of pandas dtypes to MySQL data types
            dtype_mapping = {
                'object': 'VARCHAR(255)',
                'int64': 'BIGINT',
                'float64': 'DECIMAL(18,6)',
                'bool': 'TINYINT(1)',
                'datetime64[ns]': 'DATETIME'
            }

            # Create column definitions using inferred types
            column_definitions = []
            for col in df.columns:
                col_dtype = str(df[col].dtype)
                mysql_type = dtype_mapping.get(col_dtype, 'VARCHAR(255)')
                column_definitions.append(f"`{col}` {mysql_type}")

            create_table_query = f"""
            CREATE TABLE `{table_name}` (
                {', '.join(column_definitions)}
            )
            """
            cursor.execute(create_table_query)
            print(f"Table '{table_name}' created successfully with inferred data types.")

            # Prepare data for insertion
            rows_to_insert = [tuple(row) for row in df.values]
            insert_query = f"""
            INSERT INTO `{table_name}` ({', '.join([f'`{col}`' for col in df.columns])})
            VALUES ({', '.join(['%s'] * len(df.columns))})
            """

            # Batch insert
            batch_size = 1000
            for i in range(0, len(rows_to_insert), batch_size):
                batch = rows_to_insert[i:i + batch_size]
                cursor.executemany(insert_query, batch)
                conn.commit()
                print(f"Inserted {len(batch)} rows into '{table_name}'.")

            cursor.close()
            conn.close()

    except Error as e:
        print(f"Error during database operation: {e}")


# Insert data from Sheet 1 and Sheet 2 into the corresponding tables
if df_1 is not None:
    insert_data_to_db(df_1, 'recv')
if df_2 is not None:
    insert_data_to_db(df_2, 'issues')
if df_3 is not None:
    insert_data_to_db(df_3, 'adjust')
    
    
    
query_issue = """
SELECT 
    Company,
    Product,
    Code,
    Issued_On,
    SUM(IssueQty) AS Total_IssueQty,
    SUM(IssueValue) AS Total_IssueValue
FROM rm_shortage.issues
WHERE Issued_On BETWEEN DATE_FORMAT(NOW(), '%Y-%m-01') AND NOW()
GROUP BY Company, Product, Code, Issued_On
ORDER BY Issued_On DESC, Company DESC;
"""

query_recv = """
SELECT 
    Company,
    Product,
    Code,
    Issued_On,
    SUM(ReceiveQty) AS Total_ReceiveQty,
    SUM(ReceiveValue) AS Total_ReceiveValue
FROM rm_shortage.recv
WHERE Issued_On BETWEEN DATE_FORMAT(NOW(), '%Y-%m-01') AND NOW()
GROUP BY Company, Product, Code, Issued_On
ORDER BY Issued_On DESC, Company DESC;
"""

# --- 3. Function to run query and return DataFrame ---
def run_query(query):
    try:
        conn = mysql.connector.connect(**db_config)
        if conn.is_connected():
            df = pd.read_sql(query, conn)
            return df
    except Error as e:
        print(f"Error: {e}")
    finally:
        if conn.is_connected():
            conn.close()

# --- 4. Execute queries ---
df_issue = run_query(query_issue)
df_recv = run_query(query_recv)

# --- 5. Preview results ---
print("Issue Data (df_issue):")
print(df_issue.head())

print("\nReceive Data (df_recv):")
print(df_recv.head())



    # Step 8
        # download the report

try:
    
    print("File loaded into DataFrame (PCS).")
    # Log initial shape and columns
    print(f"PCS DataFrame shape: {df_issue.shape}, Columns: {list(df_issue.columns)}")
    # Check if DataFrame has any non-header rows with data (exclude all-NaN or all-empty rows)
    df_issue_cleaned = df_issue.dropna(how='all')  # Remove rows where ALL values are NaN
    has_issue_data = not df_issue_cleaned.empty and df_issue_cleaned.shape[0] > 0
    print(f"PCS DataFrame has valid data rows: {has_issue_data}")
except Exception as e:
    print(f"Error loading PCS Excel sheet: {e}")
    df_issue = pd.DataFrame()
    has_data_pcs = False

try:
    print("File loaded into DataFrame (USD).")
    # Log initial shape and columns
    print(f"USD DataFrame shape: {df_recv.shape}, Columns: {list(df_recv.columns)}")
    # Check if DataFrame has any non-header rows with data (exclude all-NaN or all-empty rows)
    df_recv_cleaned = df_recv.dropna(how='all')  # Remove rows where ALL values are NaN
    has_recv_data = not df_recv_cleaned.empty and df_recv_cleaned.shape[0] > 0
    print(f"USD DataFrame has valid data rows: {has_recv_data}")
except Exception as e:
    print(f"Error loading USD Excel sheet: {e}")
    df_recv_cleaned = pd.DataFrame()
    has_recv_data = False

    # Setup Google Sheets API
scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
try:
    creds = service_account.Credentials.from_service_account_file('Credential.json', scopes=scope)
    log.info("✅ Successfully loaded credentials.")
except Exception as e:
    print(f"Error loading credentials: {e}")
    exit(1)

# Use gspread to authorize and access Google Sheets
client = gspread.authorize(creds)

# Open the sheet for PCS data
try:
    sheet_issue = client.open_by_key("1R14HHEKtvjCznMmfhldZDppPRwznhNgM0c0P_3M54k4")
    worksheet_issue = sheet_issue.worksheet("DF_ISSUE")
    if not has_issue_data:
        print("Skip: DataFrame (PCS) is empty or contains no valid data rows, not pasting to sheet.")
    else:
        # Clear old content and paste new data, preserving all columns
        worksheet_issue.clear()
        set_with_dataframe(worksheet_issue, df_issue, include_index=False)
        print("Data pasted to Google Sheet (Prod Data).")
        # Add timestamp to AC2
        local_tz = pytz.timezone('Asia/Dhaka')
        local_time = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
        worksheet_issue.update("AC2", [[f"{local_time}"]])
        print(f"Timestamp written to AC2: {local_time}")
except Exception as e:
    print(f"Error updating PCS sheet: {e}")

    # Open the sheet for USD data
try:
    sheet_recv = client.open_by_key("1R14HHEKtvjCznMmfhldZDppPRwznhNgM0c0P_3M54k4")
    worksheet_recv = sheet_recv.worksheet("DF_RECV")
    if not has_recv_data:
        print("Skip: DataFrame (USD) is empty or contains no valid data rows, not pasting to sheet.")
    else:
        # Clear old content and paste new data, preserving all columns
        worksheet_recv.batch_clear(['A:AC'])
        set_with_dataframe(worksheet_recv, df_recv, include_index=False)
        print("Data pasted to Google Sheet (Prod Value).")
        # Add timestamp to AC2
        local_time1 = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
        worksheet_recv.update("AC2", [[f"{local_time1}"]])
        print(f"Timestamp written to AC2: {local_time1}")
except Exception as e:
    print(f"Error updating USD sheet: {e}")