from azure.storage.blob import BlobServiceClient
import os
from typing import Union, BinaryIO
from dotenv import load_dotenv

def blb_upload_file_to_blob(
    file_path_or_stream: Union[str, BinaryIO],
    # account_name: str,
    # account_key: str,
    container_name: str,
    blob_name: str,
    overwrite: bool = True
) -> str:
    
    load_dotenv()

    # Get the subscription key from environment variables
    account_name = os.getenv('BLOB_STORAGE_ACCOUNT_NAME')
    if not account_name:
        raise ValueError("BLOB_STORAGE_ACCOUNT_NAME not found in environment variables")
    account_key = os.getenv('BLOB_STORAGE_ACCOUNT_KEY')
    if not account_key:
        raise ValueError("BLOB_STORAGE_ACCOUNT_KEY not found in environment variables")
    if not container_name:
        raise ValueError("Container name must be provided")
    if not blob_name:
        raise ValueError("Blob name must be provided")

    try:
        # Create BlobServiceClient using account key
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key
        )
        
        # Get blob client
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name
        )
        
        # Upload the file
        if isinstance(file_path_or_stream, str):
            # Upload from file path
            with open(file_path_or_stream, "rb") as data:
                blob_client.upload_blob(data, overwrite=overwrite)
        else:
            # Upload from file-like object
            blob_client.upload_blob(file_path_or_stream, overwrite=overwrite)
        
        return blob_client.url
        
    except Exception as e:
        raise Exception(f"Failed to upload file to blob storage: {str(e)}")

def blb_get_blob_size(
    container_name: str,
    blob_name: str
) -> int:

    load_dotenv()

    # Get the subscription key from environment variables
    account_name = os.getenv('BLOB_STORAGE_ACCOUNT_NAME')
    if not account_name:
        raise ValueError("BLOB_STORAGE_ACCOUNT_NAME not found in environment variables")
    account_key = os.getenv('BLOB_STORAGE_ACCOUNT_KEY')
    if not account_key:
        raise ValueError("BLOB_STORAGE_ACCOUNT_KEY not found in environment variables")
    if not container_name:
        raise ValueError("Container name must be provided")
    if not blob_name:
        raise ValueError("Blob name must be provided")

    try:
        # Create BlobServiceClient using account key
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key
        )
        
        # Get blob client
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name
        )
        
        # Get blob properties
        blob_properties = blob_client.get_blob_properties()
        
        return blob_properties.size
        
    except Exception as e:
        raise Exception(f"Failed to get blob size: {str(e)}")

# if __name__ == "__main__":
#     # Example usage
#     try:
#         url = upload_file_to_blob(
#             file_path_or_stream="requirements.txt",
#             container_name="directory-web-pages",
#             blob_name="uploaded_test.txt"
#         )
#         print(f"File uploaded successfully. Blob URL: {url}")
#     except Exception as e:
#         print(f"Error: {e}")