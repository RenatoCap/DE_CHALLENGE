import os
import pyodbc
import logging
import datetime

from pathlib import Path
from dotenv import load_dotenv
from fastavro import writer, parse_schema
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# --- Configuration and Environment Variables ---
CONNECTION_PATH = os.path.join(Path(__file__).parents[1], 'config', 'connections.env')

# Define the base log folder and the specific backup log folder
LOG_BASE_FOLDER = os.path.join(Path(__file__).parents[1], 'log')
LOG_BACKUP_FOLDER = os.path.join(LOG_BASE_FOLDER, 'backup')

# Load environment variables from the specified connections.env file.
load_dotenv(CONNECTION_PATH)

# --- Logging Setup ---
os.makedirs(LOG_BACKUP_FOLDER, exist_ok=True)

log_file_name = datetime.datetime.now().strftime("backup_process_%Y%m%d_%H%M%S.log")
log_file_path = os.path.join(LOG_BACKUP_FOLDER, log_file_name)

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# --- Azure Blob Storage Credentials ---
BLOB_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME_HISTORIC")

# --- Database Credentials ---
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "1433") # Default port remains 1433

# --- Environment Variable Validation ---
if not all([BLOB_CONNECTION_STRING, BLOB_CONTAINER_NAME, DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD]):
    logger.error("ERROR: Please ensure all required environment variables are set.")
    logger.error("Required: AZURE_STORAGE_CONNECTION_STRING, BLOB_CONTAINER_NAME_HISTORIC, DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD")
    exit(1)


def get_blob_service_client():
    """
    Establishes and returns an Azure Blob Storage service client.

    This client is used to interact with Azure Blob Storage, allowing operations
    such as listing containers, uploading blobs, and downloading blobs.

    :return: An instance of BlobServiceClient connected to Azure Blob Storage.
    """
    logger.info("Attempting to get Azure Blob Storage service client...")
    try:
        client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        logger.info("Successfully obtained BlobServiceClient.")
        return client
    except Exception as e:
        logger.error(f"Failed to get BlobServiceClient: {e}", exc_info=True)
        return None


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
        logger.info("Successfully connected to SQL Server.")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}", exc_info=True)
        return None


def export_table_to_avro(table_full_name, schema, container_name):
    """
    Exports data from a specified SQL table to AVRO format and uploads it to Azure Blob Storage.

    :param table_full_name: The full name of the table.
    :param schema: The AVRO schema for the table data.
    :param container_name: The name of the Azure Blob Storage container.
    """
    logger.info(f"Starting export process for table: {table_full_name}")
    avro_schema = parse_schema(schema)
    dataset_name, table_name = table_full_name.split('.')

    blob_service_client = get_blob_service_client()
    if not blob_service_client:
        logger.error("BlobServiceClient not available. Aborting export.")
        return

    db_conn = get_sql_server_connection()
    if not db_conn:
        logger.error("Could not connect to the database. Aborting export.")
        return

    avro_file_name = f'{table_name}_{datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.avro'

    temp_dir = os.path.join(Path(__file__).parents[1], 'tmp')
    if not os.path.exists(temp_dir):
        logger.info(f"Creating temporary directory: {temp_dir}")
        os.makedirs(temp_dir, exist_ok=True)
    else:
        logger.info(f"Temporary directory already exists: {temp_dir}")

    local_avro_path = os.path.join(temp_dir, avro_file_name)
    cursor = None
    try:
        cursor = db_conn.cursor()
        logger.info(f"Executing query: SELECT * FROM {dataset_name}.{table_name}")
        cursor.execute(f"SELECT * FROM [{dataset_name}].[{table_name}]")
        records = []
        columns = [column[0] for column in cursor.description]

        for row in cursor:
            record = {}
            for i, col_name in enumerate(columns):
                val = row[i]
                if isinstance(val, (bytes, bytearray)):
                    val = val.decode('utf-8')
                elif val is None: 
                    pass
                elif isinstance(val, datetime.datetime):
                    val = int(val.timestamp() * 1000)
                record[col_name.lower()] = val
            records.append(record)

        logger.info(f"Extracted {len(records)} records from {table_full_name}.")

        if not records:
            logger.warning("No records found to export. Skipping AVRO file creation and upload.")
            return

        logger.info(f"Writing {len(records)} records to local AVRO file: {local_avro_path}")
        with open(local_avro_path, 'wb') as out_file:
            writer(out_file, avro_schema, records)
        logger.info("Local AVRO file created successfully.")

        logger.info(f"Uploading AVRO file '{avro_file_name}' to Blob Storage container '{container_name}'...")
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(f"{dataset_name}/{table_name}/{avro_file_name}")
        with open(local_avro_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        logger.info(f"Successfully uploaded {avro_file_name} to {container_name}/{dataset_name}/{table_name}/ in Blob Storage.")

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        logger.error(f"SQL Error during export: {sqlstate} - {ex}", exc_info=True)
        db_conn.rollback()
    except Exception as e:
        logger.error(f"An unexpected error occurred during export: {e}", exc_info=True)
    finally:
        if cursor:
            cursor.close()
        if db_conn:
            db_conn.close()
            logger.info("SQL connection closed.")
        if os.path.exists(local_avro_path):
            os.remove(local_avro_path)
            logger.info(f"Local temporary file '{local_avro_path}' deleted.")


def main():
    logger.info("--- Starting main export process ---")
    table_name_jobs = "migration_tables.jobs"
    schema_jobs = {
        "type": "record",
        "name": "Job",
        "namespace": "migration_tables",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "job", "type": "string"}
        ]
    }
    container_name_backup = "backup"

    export_table_to_avro(table_full_name=table_name_jobs, schema=schema_jobs, container_name=container_name_backup)

    table_name_departments = "migration_tables.departments"
    schema_departments = {
        "type": "record",
        "name": "Department",
        "namespace": "migration_tables",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "department", "type": "string"}
        ]
    }
    export_table_to_avro(table_full_name=table_name_departments, schema=schema_departments, container_name=container_name_backup)

    table_name_employees = "migration_tables.hired_employees"
    schema_employees = {
        "type": "record",
        "name": "HiredEmployee",
        "namespace": "migration_tables",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": ["null", "string"]},
            {"name": "datetime", "type": ["null", "string"]},
            {"name": "department_id", "type": ["null", "int"]},
            {"name": "job_id", "type": ["null", "int"]}
        ]
    }
    export_table_to_avro(table_full_name=table_name_employees, schema=schema_employees, container_name=container_name_backup)

    logger.info("--- Export process completed ---")


# --- Main execution block ---
if __name__ == "__main__":
    main()