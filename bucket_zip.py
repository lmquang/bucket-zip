import logging
from google.cloud import storage
import zipfile
import io
import sys
import os
import base64
import json
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Configure logging to write to a file
logging.basicConfig(filename='bucket_zip.log', level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Also log to console
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

# Load environment variables from .env file if it exists
load_dotenv()

def process_blob(blob):
    """
    Process a single blob and return its content and size.

    Args:
        blob (google.cloud.storage.blob.Blob): The blob to process.

    Returns:
        tuple: A tuple containing the blob name, its content, and its size.
    """
    logging.info(f"Processing file: {blob.name}")
    content = blob.download_as_bytes()
    return blob.name, content, blob.size

def format_size(size_bytes):
    """
    Format the size in bytes to a human-readable format.

    Args:
        size_bytes (int): Size in bytes.

    Returns:
        str: Formatted size string.
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0

def zip_and_upload_bucket(source_bucket_name, destination_bucket_name, max_workers=10, chunk_size=1000):
    """
    Zip the contents of a source GCS bucket and upload the resulting zip file to a destination GCS bucket.

    Args:
        source_bucket_name (str): Name of the source GCS bucket.
        destination_bucket_name (str): Name of the destination GCS bucket.
        max_workers (int): Maximum number of concurrent workers.
        chunk_size (int): Number of blobs to process in each chunk.

    Raises:
        Exception: If any error occurs during the process.
    """
    try:
        logging.info(f"Starting zip_and_upload_bucket with source: {source_bucket_name}, destination: {destination_bucket_name}")
        
        # Get the base64 encoded service account JSON from environment variable
        service_account_json_b64 = os.getenv('GCP_SA_KEY')
        if not service_account_json_b64:
            raise ValueError("GCP_SA_KEY environment variable is not set")

        # Decode the base64 encoded JSON
        service_account_json = base64.b64decode(service_account_json_b64).decode('utf-8')
        service_account_info = json.loads(service_account_json)

        # Initialize GCS client with service account info
        storage_client = storage.Client.from_service_account_info(service_account_info)
        logging.info(f"Initialized GCS client using service account info from environment variable")

        # Get source and destination buckets
        source_bucket = storage_client.bucket(source_bucket_name)
        destination_bucket = storage_client.bucket(destination_bucket_name)
        logging.info(f"Retrieved source bucket: {source_bucket_name} and destination bucket: {destination_bucket_name}")

        # Create a temporary zip file in memory
        zip_buffer = io.BytesIO()
        zip_lock = Lock()
        
        total_size_before = 0
        total_files = 0
        processed_files = 0

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # Process blobs in chunks
            blobs_iterator = source_bucket.list_blobs()
            
            for page in blobs_iterator.pages:
                chunk = list(page)
                total_files += len(chunk)
                logging.info(f"Processing chunk of {len(chunk)} files. Total files so far: {total_files}")
                
                # Process blobs concurrently
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_blob = {executor.submit(process_blob, blob): blob for blob in chunk}
                    for future in as_completed(future_to_blob):
                        blob = future_to_blob[future]
                        try:
                            blob_name, content, blob_size = future.result()
                            with zip_lock:
                                zip_file.writestr(blob_name, content)
                                total_size_before += blob_size
                                processed_files += 1
                                if processed_files % 100 == 0:
                                    logging.info(f"Progress: {processed_files}/{total_files} files processed")
                        except Exception as exc:
                            logging.error(f"Error processing {blob.name}: {exc}")
        
        total_size_after = zip_buffer.tell()
        compression_ratio = (1 - (total_size_after / total_size_before)) * 100 if total_size_before > 0 else 0
        
        logging.info(f"Finished creating zip file in memory. Total files processed: {processed_files}/{total_files}")
        logging.info(f"Total size before compression: {format_size(total_size_before)}")
        logging.info(f"Total size after compression: {format_size(total_size_after)}")
        logging.info(f"Compression ratio: {compression_ratio:.2f}%")

        # Upload the zip file to the destination bucket
        zip_buffer.seek(0)
        destination_blob = destination_bucket.blob(f'bucket-{source_bucket_name}.zip')
        destination_blob.upload_from_file(zip_buffer, content_type='application/zip')
        logging.info(f"Uploaded zip file to {destination_bucket_name}/bucket-{source_bucket_name}.zip")

        # Clean up
        zip_buffer.close()
        logging.info("Cleaned up resources")

        logging.info("Operation completed successfully")
    except Exception as e:
        logging.exception(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    logging.info("Script started")
    if len(sys.argv) != 3:
        logging.error("Incorrect number of arguments")
        print("Usage: python bucket_zip.py <source_bucket_name> <destination_bucket_name>")
        sys.exit(1)
    
    source_bucket_name = sys.argv[1]
    destination_bucket_name = sys.argv[2]
    
    logging.info(f"Arguments received: source_bucket={source_bucket_name}, destination_bucket={destination_bucket_name}")
    
    try:
        zip_and_upload_bucket(source_bucket_name, destination_bucket_name)
    except Exception as e:
        logging.exception(f"Failed to zip and upload bucket: {str(e)}")
        sys.exit(1)
    
    logging.info("Script completed")