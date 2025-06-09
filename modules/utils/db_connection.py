import os
import pyodbc

from pathlib import Path
from dotenv import load_dotenv

# --- Configuration and Environment Variables ---
CONNECTION_PATH = os.path.join(Path(__file__).parents[2], 'config', 'connections.env')
LOG_FOLDER = os.path.join(Path(__file__).parents[2], 'log')

# Load environment variables from the specified connections.env file.
load_dotenv(CONNECTION_PATH) 

# --- Database Credentials ---
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "1433")


def get_sql_server_connection():
    """
    Establishes and returns a connection to the SQL Server database.

    The connection string is built using environment variables to ensure secure
    and flexible database access. Autocommit is set too False to allow for explicit
    transaction management (commit/rollback).

    :return: A pyodbc connection object if successful, None otherwise.
    """
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={DB_SERVER};"
            f"DATABASE={DB_NAME};"
            f"UID={DB_USER};"
            f"PWD={DB_PASSWORD};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )
        conn = pyodbc.connect(conn_str)
        conn.autocommit = False
        return conn
    except Exception as e:
        return None