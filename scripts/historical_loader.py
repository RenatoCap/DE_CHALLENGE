import os
import io
import json
import pyodbc
import logging
import argparse
import pandas as pd

from pathlib import Path
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# --- Configuration and Environment Variables ---
CONNECTION_PATH = os.path.join(Path(__file__).parents[1], 'config', 'connections.env')
LOG_FOLDER = os.path.join(Path(__file__).parents[1], 'log')

# Load environment variables from the specified connections.env file.
load_dotenv(CONNECTION_PATH) 

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set up a dedicated logger for rejected rows to output to a separate log file.
rejected_rows_logger = logging.getLogger('rejected_rows_logger')
if not rejected_rows_logger.handlers:
    # Use FileHandler to write to a separate file
    file_handler = logging.FileHandler(os.path.join(LOG_FOLDER, 'rejected_hired_employees.log'), mode='a', encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    file_handler.setFormatter(formatter)
    rejected_rows_logger.addHandler(file_handler)
    rejected_rows_logger.setLevel(logging.WARNING)

# --- Azure Blob Storage Credentials ---
BLOB_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME_HISTORIC")

# --- Database Credentials ---
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "1433")

# --- Environment Variable Validation ---
if not all([BLOB_CONNECTION_STRING, BLOB_CONTAINER_NAME, DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD]):
    logger.error("ERROR: Please ensure all required environment variables are set.")
    logger.error("Required: AZURE_STORAGE_CONNECTION_STRING, BLOB_CONTAINER_NAME_HISTORIC, DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD")
    exit(1)

# --- Helper Functions ---

def get_blob_service_client():
    """
    Establishes and returns an Azure Blob Storage service client.

    This client is used to interact with Azure Blob Storage, allowing operations
    such as listing containers, uploading blobs, and downloading blobs.

    :return: An instance of BlobServiceClient connected to Azure Blob Storage.
    """
    return BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)

def get_sql_server_connection():
    """
    Establishes and returns a connection to the SQL Server database.

    The connection string is built using environment variables to ensure secure
    and flexible database access. Autocommit is set to False to allow for explicit
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
        logger.info("Successfully connected to SQL Server.")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}", exc_info=True)
        return None

def ingest_csv_to_db(csv_file_content: bytes, db_conn, table_name: str, target_columns: list, insert_columns: list) -> int:
    """
    Reads the contents of a CSV file, processes it, and inserts valid rows into
    the specified SQL Server table.

    This function performs the following steps:
    1. Decodes the CSV content and reads it into a pandas DataFrame.
    2. Filters out rows that have null values in any of the `insert_columns`.
    3. Logs discarded rows to a separate file for auditing.
    4. Prepares the filtered data for insertion.
    5. Inserts the valid data into the target database table using `executemany`
       for efficiency.
    6. Commits the transaction if successful, rolls back on error.

    :param csv_file_content: The content of the CSV file as bytes.
    :param db_conn: An active pyodbc database connection object.
    :param table_name: The name of the target table in the database.
    :param target_columns: A list of column names expected in the CSV file, in order.
    :param insert_columns: A list of column names to be inserted into the database table.

    :return: The number of rows successfully inserted into the database.
    :raises Exception: If an error occurs during processing or insertion,
                       the exception is re-raised after logging and rollback.
    """
    try:
        # Read CSV into a DataFrame, using target_columns for naming and no header.
        df = pd.read_csv(io.StringIO(csv_file_content.decode('utf-8')),
                         names=target_columns,
                         header=None,
                         index_col=False)

        # Create a combined filter to identify rows with nulls in any INSERT_COLUMNS.
        combined_filter = pd.Series(True, index=df.index)
        for col in insert_columns:
            if col in df.columns:
                combined_filter = combined_filter & df[col].notna()
            else:
                logger.warning(f"The column '{col}' specified in INSERT_COLUMNS was not found in the DataFrame. The filter will not be applied for this column.")

        # Separate DataFrame into filtered (valid) and rejected (invalid) rows.
        df_filtered = df[combined_filter].reset_index(drop=True).copy()
        df_rejected = df[~combined_filter].copy()

        rejected_rows_count = len(df_rejected)

        if rejected_rows_count > 0:
            logger.warning(f"Se encontraron y descartaron {rejected_rows_count} filas debido a valores nulos en columnas clave. Consulta 'rejected_hired_employees.log'.")
            
            # Log each rejected row as a JSON object for easier parsing.
            for _, row in df_rejected.iterrows():
                rejected_data = row.astype(str).to_dict()
                rejected_rows_logger.warning(json.dumps(rejected_data))

        data_to_insert = [tuple(row) for row in df_filtered[insert_columns].values]
        
        if not data_to_insert:
            logger.info("There are no rows to insert after processing.")
            return 0

        # Prepare and execute the bulk insert.
        cursor = db_conn.cursor()
        placeholders = ', '.join(['?' for _ in insert_columns])
        insert_sql = f"INSERT INTO {table_name} ({', '.join(insert_columns)}) VALUES ({placeholders})"
        cursor.executemany(insert_sql, data_to_insert)
        db_conn.commit()
        cursor.close()
        logger.info(f"Inserted {len(data_to_insert)} rows in '{table_name}'.")
        return len(data_to_insert)

    except Exception as e:
        logger.error(f"Error processing and inserting data: {e}", exc_info=True)
        if db_conn:
            db_conn.rollback()
            logger.info("Database transaction rolled back.")
        raise

# --- Main Execution Flow ---

def main(file_name, target_table_name, target_columns, insert_columns):
    """
    Main function to orchestrate the data ingestion process.

    This function performs the following high-level steps:
    1. Connects to Azure Blob Storage.
    2. Connects to the SQL Server database.
    3. Iterates through blobs in the specified container, looking for the target CSV file.
    4. Downloads and processes each relevant CSV file, ingesting its data into the database.
    5. Logs overall progress and summarizes the results.
    6. Ensures database connection is closed gracefully.

    :param file_name: The name of the CSV file to look for in the Blob Storage.
    :param target_table_name: The name of the database table to insert data into.
    :param target_columns: List of column names expected in the CSV.
    :param insert_columns: List of column names to insert into the database.
    """
    blob_service_client = get_blob_service_client()
    container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
    logger.info(f"Connected to the Blob Storage container: {BLOB_CONTAINER_NAME}")

    db_conn = get_sql_server_connection()
    if not db_conn:
        logger.error("Could not connect to the database. Aborting.")
        return

    total_files_processed = 0
    total_rows_ingested = 0

    try:
        blob_list = container_client.list_blobs()
        
        # Iterate over each blob in the container.
        for blob in blob_list:
            if file_name.lower() in blob.name.lower():
                logger.info(f"Processing CSV file: {blob.name}")
                
                try:
                    blob_client = container_client.get_blob_client(blob.name)
                    download_stream = blob_client.download_blob().readall()
                    
                    rows_ingested_from_file = ingest_csv_to_db(download_stream, db_conn, target_table_name, target_columns, insert_columns)
                    total_rows_ingested += rows_ingested_from_file
                    total_files_processed += 1
                    
                except Exception as file_e:
                    logger.error(f"Failed to process file {blob.name}: {file_e}", exc_info=True)

        logger.info("--- Historical Ingestion Process Completed ---")
        logger.info(f"Processed CSV files: {total_files_processed}")
        logger.info(f"Total rows inserted: {total_rows_ingested}")

    except Exception as e:
        logger.error(f"General error during the migration process: {e}", exc_info=True)
    finally:
        if db_conn:
            db_conn.close()
            logger.info("Connection to SQL Server closed.")

# --- Script Entry Point ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data from Azure Blob Storage to SQL Server.")
    
    parser.add_argument("--file_name", type=str,
                        help=f'The name of the CSV file to ingest.')
    parser.add_argument("--target_table_name", type=str,
                        help=f'The name of the target SQL Server table.')
    parser.add_argument("--target_columns", type=str,
                        help=f'Comma-separated list of column names in the CSV.')
    parser.add_argument("--insert_columns", type=str,
                        help=f'Comma-separated list of column names to insert into DB.')
    args = parser.parse_args()

    parsed_target_columns = [col.strip() for col in args.target_columns.split(',')]
    parsed_insert_columns = [col.strip() for col in args.insert_columns.split(',')]

    main(args.file_name, args.target_table_name, parsed_target_columns, parsed_insert_columns)
