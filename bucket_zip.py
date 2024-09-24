import logging
from google.cloud import storage
import zipfile
import io
import sys
import os
import base64
import json
from dotenv import load_dotenv
import argparse
import gc
from memory_profiler import profile
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bucket_zip.log'),
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()

def format_size(size_bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0

def process_blob(blob, result_queue):
    """Process a single blob and add it to the result queue."""
    try:
        logging.info(f"Processing file: {blob.name}, size: {format_size(blob.size)}")
        with blob.open("rb") as f:
            content = f.read()
        result_queue.put((blob.name, content))
    except Exception as exc:
        logging.error(f"Error processing {blob.name}: {exc}")

@profile
def zip_and_upload_page(page_blobs, destination_bucket, source_bucket_name, page_number, max_workers):
    zip_buffer = io.BytesIO()
    result_queue = Queue()
    processed_files = 0
    zip_chunk_number = 1
    zip_size = 0
    MAX_ZIP_SIZE = 1024 * 1024 * 1024  # 1GB in bytes

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_blob, blob, result_queue) for blob in page_blobs]
        
        for future in as_completed(futures):
            future.result()  # This will raise any exceptions that occurred during processing
            
            while not result_queue.empty():
                file_name, content = result_queue.get()
                
                # Check if adding this file would exceed the max zip size
                if zip_size + len(content) > MAX_ZIP_SIZE and zip_size > 0:
                    # Upload the current zip file
                    zip_buffer.seek(0)
                    destination_blob = destination_bucket.blob(f'{source_bucket_name}/page_{page_number:05d}_chunk_{zip_chunk_number:05d}.zip')
                    destination_blob.upload_from_file(zip_buffer, content_type='application/zip')
                    logging.info(f"Uploaded zip file for page {page_number}, chunk {zip_chunk_number}")
                    
                    # Start a new zip file
                    zip_buffer.close()
                    zip_buffer = io.BytesIO()
                    zip_chunk_number += 1
                    zip_size = 0
                
                # Add the file to the current zip
                with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED) as zip_file:
                    zip_file.writestr(file_name, content)
                
                zip_size += len(content)
                processed_files += 1
                
                if processed_files % 100 == 0:
                    logging.info(f"Progress: {processed_files} files processed in page {page_number}")
                    gc.collect()  # Explicitly call garbage collection

    # Upload the last zip file if it's not empty
    if zip_size > 0:
        zip_buffer.seek(0)
        destination_blob = destination_bucket.blob(f'{source_bucket_name}/page_{page_number:05d}_chunk_{zip_chunk_number:05d}.zip')
        destination_blob.upload_from_file(zip_buffer, content_type='application/zip')
        logging.info(f"Uploaded final zip file for page {page_number}, chunk {zip_chunk_number}")

    logging.info(f"Finished creating zip files for page {page_number}. Total files processed: {processed_files}")
    logging.info(f"Total chunks created for page {page_number}: {zip_chunk_number}")

    # Clean up
    zip_buffer.close()
    gc.collect()

    return zip_chunk_number

@profile
def zip_and_upload_bucket(source_bucket_name, destination_bucket_name, max_workers=10):
    try:
        logging.info(f"Starting zip_and_upload_bucket with source: {source_bucket_name}, destination: {destination_bucket_name}")
        
        # Initialize GCS client
        service_account_json_b64 = os.getenv('GCP_SA_KEY')
        if not service_account_json_b64:
            raise ValueError("GCP_SA_KEY environment variable is not set")
        service_account_json = base64.b64decode(service_account_json_b64).decode('utf-8')
        service_account_info = json.loads(service_account_json)
        storage_client = storage.Client.from_service_account_info(service_account_info)
        
        source_bucket = storage_client.bucket(source_bucket_name)
        destination_bucket = storage_client.bucket(destination_bucket_name)
        
        page_number = 0
        manifest = []
        for page in source_bucket.list_blobs().pages:
            page_number += 1
            logging.info(f"Processing page {page_number}")
            chunk_count = zip_and_upload_page(page, destination_bucket, source_bucket_name, page_number, max_workers)
            
            # Add chunk information to manifest
            for chunk in range(1, chunk_count + 1):
                manifest.append(f"page_{page_number:05d}_chunk_{chunk:05d}.zip")

        # Create a manifest file
        manifest_content = f"Total pages: {page_number}\n"
        manifest_content += "\n".join(manifest)
        destination_blob = destination_bucket.blob(f'{source_bucket_name}/manifest.txt')
        destination_blob.upload_from_string(manifest_content)
        logging.info("Uploaded manifest file")
        
        logging.info("Operation completed successfully")
    except Exception as e:
        logging.exception(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    logging.info("Script started")
    
    parser = argparse.ArgumentParser(description="Zip and upload GCS bucket contents")
    parser.add_argument("source_bucket", help="Name of the source GCS bucket")
    parser.add_argument("destination_bucket", help="Name of the destination GCS bucket")
    parser.add_argument("--max-workers", type=int, default=10, help="Maximum number of concurrent workers")
    
    args = parser.parse_args()
    
    logging.info(f"Arguments received: source_bucket={args.source_bucket}, destination_bucket={args.destination_bucket}, "
                 f"max_workers={args.max_workers}")
    
    try:
        zip_and_upload_bucket(args.source_bucket, args.destination_bucket, args.max_workers)
    except Exception as e:
        logging.exception(f"Failed to zip and upload bucket: {str(e)}")
        sys.exit(1)
    
    logging.info("Script completed")