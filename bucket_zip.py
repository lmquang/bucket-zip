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
import argparse

# Configure logging to write to a file and console
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bucket_zip.log'),
        logging.StreamHandler()
    ]
)

# Load environment variables from .env file if it exists
load_dotenv()

def process_blob(blob):
    """
    Process a single blob and return its content.

    Args:
        blob (google.cloud.storage.blob.Blob): The blob to process.

    Returns:
        tuple: A tuple containing the blob name and its content.
    """
    logging.info(f"Processing file: {blob.name}, size: {format_size(blob.size)}")
    content = blob.download_as_bytes()
    return blob.name, content

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

def split_zip(zip_buffer, max_part_size):
    """
    Split a zip file into parts of specified maximum size.

    Args:
        zip_buffer (io.BytesIO): The buffer containing the zip file.
        max_part_size (int): Maximum size of each part in bytes.

    Returns:
        list: A list of tuples, each containing a part name and its content.
    """
    zip_buffer.seek(0)
    zip_content = zip_buffer.getvalue()
    parts = []
    for i in range(0, len(zip_content), max_part_size):
        part = zip_content[i:i+max_part_size]
        part_name = f"bucket_contents.zip.part{i//max_part_size+1}"
        parts.append((part_name, part))
    return parts

def zip_and_upload_bucket(source_bucket_name, destination_bucket_name, max_zip_size_mb=1024, max_workers=10):
    """
    Zip the contents of a source GCS bucket and upload the resulting zip file to a destination GCS bucket.

    Args:
        source_bucket_name (str): Name of the source GCS bucket.
        destination_bucket_name (str): Name of the destination GCS bucket.
        max_zip_size_mb (int): Maximum size of the zip file before splitting, in MB.
        max_workers (int): Maximum number of concurrent workers.

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

        # Create a zip file in memory
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # Process blobs concurrently
            blobs = list(source_bucket.list_blobs())
            total_files = len(blobs)
            processed_files = 0
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_blob = {executor.submit(process_blob, blob): blob for blob in blobs}
                for future in as_completed(future_to_blob):
                    blob = future_to_blob[future]
                    try:
                        blob_name, blob_content = future.result()
                        zip_file.writestr(blob_name, blob_content)
                        processed_files += 1
                        if processed_files % 100 == 0:
                            logging.info(f"Progress: {processed_files}/{total_files} files processed")
                    except Exception as exc:
                        logging.error(f"Error processing {blob.name}: {exc}")

        zip_size = zip_buffer.tell()
        logging.info(f"Finished creating zip file in memory. Total files processed: {processed_files}/{total_files}")
        logging.info(f"Total size of zip file: {format_size(zip_size)}")

        max_zip_size_bytes = max_zip_size_mb * 1024 * 1024
        if zip_size > max_zip_size_bytes:
            # Split the zip file into parts
            logging.info(f"Zip file size ({format_size(zip_size)}) exceeds maximum size ({format_size(max_zip_size_bytes)}). Splitting into parts.")
            parts = split_zip(zip_buffer, max_zip_size_bytes)
            for part_name, part_content in parts:
                destination_blob = destination_bucket.blob(source_bucket_name+"/"+part_name)
                destination_blob.upload_from_string(part_content)
                logging.info(f"Uploaded part: {part_name}")
        else:
            # Upload the whole zip file
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
    
    parser = argparse.ArgumentParser(description="Zip and upload GCS bucket contents")
    parser.add_argument("source_bucket", help="Name of the source GCS bucket")
    parser.add_argument("destination_bucket", help="Name of the destination GCS bucket")
    parser.add_argument("--max-zip-size", type=int, default=1024, help="Maximum size of the zip file before splitting (in MB)")
    parser.add_argument("--max-workers", type=int, default=10, help="Maximum number of concurrent workers")
    
    args = parser.parse_args()
    
    if args.max_zip_size < 1:
        parser.error("Maximum zip size must be at least 1 MB")
    
    logging.info(f"Arguments received: source_bucket={args.source_bucket}, destination_bucket={args.destination_bucket}, "
                 f"max_zip_size={args.max_zip_size} MB, max_workers={args.max_workers}")
    
    try:
        zip_and_upload_bucket(args.source_bucket, args.destination_bucket, args.max_zip_size, args.max_workers)
    except Exception as e:
        logging.exception(f"Failed to zip and upload bucket: {str(e)}")
        sys.exit(1)
    
    logging.info("Script completed")