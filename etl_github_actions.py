import os
import json
import base64
import pandas as pd
import mysql.connector
from mysql.connector import Error
import re
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account
from datetime import datetime
import pytz
import logging
import time
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type  # Add this import

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

# ----------------- Database config -----------------
db_config = {
    'host': '127.0.0.1',  # inside GitHub Actions runner
    'database': 'rm_shortage',
    'user': 'root',
    'password': 'root',
    'port': 3306
}

# ----------------- Google Sheets setup -----------------
GSPREAD_CREDS_BASE64 = os.getenv('GSPREAD_CREDENTIALS')
if not GSPREAD_CREDS_BASE64:
    raise ValueError("Missing GSPREAD_CREDENTIALS secret in GitHub Actions")

# Decode Base64 secret
GSPREAD_CREDS_JSON = base64.b64decode(GSPREAD_CREDS_BASE64).decode('utf-8')
creds_dict = json.loads(GSPREAD_CREDS_JSON)

scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scope)
client = gspread.authorize(creds)
log.info("✅ Google Sheets authorized")

# ----------------- Sheet CSV URLs -----------------
sheet_ids = {
    'recv': "1oGev0ICLnkDxEPs-wNKE-GMraHQX57ELODgReTINPJ4",
    'issues': "1kK6b4IPJz5y6ESWqvxGGk6KUsiDXOOjyaK4IRo0UzxU",
    'adjust': "1-OwCIObhsTk25-t0AcyyI92Vn7HihRTDjnBdagp5iAA"
}
sheet_name = "Sheet1"

def csv_url(sheet_id):
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

# ----------------- Helper functions -----------------
def clean_column_name(col):
    col = col.strip()
    col = re.sub(r'[^A-Za-z0-9_]+', '', col)
    return col.replace(' ', '_').replace('-', '_')

def load_data(csv_url):
    df = pd.read_csv(csv_url, low_memory=False)
    df.columns = [clean_column_name(c) for c in df.columns]
    df = df.fillna('')
    if df.columns[0] != 'Issued_On':
        df.rename(columns={df.columns[0]: 'Issued_On'}, inplace=True)
    return df

def create_connection():
    try:
        conn = mysql.connector.connect(**db_config)
        if conn.is_connected():
            return conn
    except Error as e:
        log.error(f"MySQL connection error: {e}")
    return None

def insert_data_to_db(df, table_name):
    conn = create_connection()
    if not conn:
        return
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    log.info(f"Dropped table {table_name} if existed")

    dtype_mapping = {'object':'VARCHAR(255)', 'int64':'BIGINT',
                     'float64':'DECIMAL(18,6)','bool':'TINYINT(1)',
                     'datetime64[ns]':'DATETIME'}

    cols_def = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        cols_def.append(f"`{col}` {dtype_mapping.get(dtype, 'VARCHAR(255)')}")
    cursor.execute(f"CREATE TABLE `{table_name}` ({', '.join(cols_def)})")
    log.info(f"Created table {table_name}")

    rows = [tuple(r) for r in df.values]
    insert_query = f"INSERT INTO `{table_name}` ({', '.join([f'`{c}`' for c in df.columns])}) VALUES ({', '.join(['%s']*len(df.columns))})"

    for i in range(0, len(rows), 1000):
        batch = rows[i:i+1000]
        cursor.executemany(insert_query, batch)
        conn.commit()
        log.info(f"Inserted {len(batch)} rows into {table_name}")

    cursor.close()
    conn.close()

def run_query(query):
    conn = create_connection()
    if not conn:
        return pd.DataFrame()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

@retry(
    stop=stop_after_attempt(5),  # Retry up to 5 times
    wait=wait_exponential(multiplier=1, min=5, max=60),  # Wait 5s, 10s, 20s, etc.
    retry=retry_if_exception_type(gspread.exceptions.APIError),  # Retry only on API errors like 503
    before_sleep=lambda retry_state: log.warning(f"Retrying due to API error: {retry_state.outcome.exception()} (attempt {retry_state.attempt_number})")
)
def paste_to_gsheet(df, sheet_key, worksheet_name):
    sheet = client.open_by_key(sheet_key)
    ws = sheet.worksheet(worksheet_name)
    time.sleep(5)  # Existing delay
    ws.clear()
    set_with_dataframe(ws, df, include_index=False)
    ts = datetime.now(pytz.timezone('Asia/Dhaka')).strftime("%Y-%m-%d %H:%M:%S")
    time.sleep(2)  # Existing delay
    ws.update(range_name="AC2", values=[[ts]])
    log.info(f"Pasted {worksheet_name} and updated timestamp {ts}")

def cleanup_db():
    conn = create_connection()
    if not conn:
        return
    cursor = conn.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {db_config['database']}")
    log.info("Dropped database successfully")
    cursor.close()
    conn.close()

# ----------------- Main ETL -----------------
try:
    # Load data from 3 Google Sheets
    dfs = {name: load_data(csv_url(sheet_id)) for name, sheet_id in sheet_ids.items()}
    for table, df in dfs.items():
        insert_data_to_db(df, table)

    # Queries for issues and recv
    query_issue = """
    SELECT Company, Product, Code, Issued_On,
    SUM(IssueQty) AS Total_IssueQty, SUM(IssueValue) AS Total_IssueValue
    FROM issues
    GROUP BY Company, Product, Code, Issued_On
    ORDER BY Issued_On DESC
    """
    query_recv = """
    SELECT Company, Product, Code, Issued_On,
    SUM(ReceiveQty) AS Total_ReceiveQty, SUM(ReceiveValue) AS Total_ReceiveValue
    FROM recv
    GROUP BY Company, Product, Code, Issued_On
    ORDER BY Issued_On DESC
    """

    df_issue_result = run_query(query_issue)
    df_recv_result = run_query(query_recv)

    # Paste results to 2 separate worksheets in the output Google Sheet
    output_sheet_key = "1R14HHEKtvjCznMmfhldZDppPRwznhNgM0c0P_3M54k4"
    paste_to_gsheet(df_issue_result, output_sheet_key, "DF_ISSUE")
    paste_to_gsheet(df_recv_result, output_sheet_key, "DF_RECV")

except Exception as e:
    log.error(f"ETL failed: {e}")
    raise  # Re-raise to fail the action if all retries exhaust

finally:
    cleanup_db()