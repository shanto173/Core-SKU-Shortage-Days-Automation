import os
import json
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
GSPREAD_CREDS = os.getenv('GOOGLE_CREDENTIALS_BASE64')
if not GSPREAD_CREDS:
    raise ValueError("Missing GSPREAD_CREDENTIALS secret in GitHub Actions")

creds_dict = json.loads(GSPREAD_CREDS)
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

def paste_to_gsheet(df, sheet_key, worksheet_name):
    sheet = client.open_by_key(sheet_key)
    ws = sheet.worksheet(worksheet_name)
    ws.clear()
    set_with_dataframe(ws, df, include_index=False)
    ts = datetime.now(pytz.timezone('Asia/Dhaka')).strftime("%Y-%m-%d %H:%M:%S")
    ws.update("AC2", [[ts]])
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

finally:
    cleanup_db()
