import os

from pathlib import Path
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient


# --- Configuration and Environment Variables ---
CONNECTION_PATH = os.path.join(Path(__file__).parents[2], 'config', 'connections.env')
LOG_FOLDER = os.path.join(Path(__file__).parents[2], 'log')

# Load environment variables from the specified connections.env file.
load_dotenv(CONNECTION_PATH) 

# --- Azure Blob Storage Credentials ---
BLOB_CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.environ.get("BLOB_CONTAINER_NAME_HISTORIC")


def get_blob_service_client():
    """
    Establishes and returns an Azure Blob Storage service client.

    This client is used to interact with Azure Blob Storage, allowing operations
    such as listing containers, uploading blobs, and downloading blobs.

    :return: An instance of BlobServiceClient connected to Azure Blob Storage.
    """
    return BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)