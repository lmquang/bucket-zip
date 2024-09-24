import argparse
import base64
import gc
import io
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from typing import List, Tuple, Set, Optional

import zipfile
from dotenv import load_dotenv
from google.cloud import storage
from memory_profiler import profile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bucket_zip.log'),
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()

def format_size(size_bytes: float) -> str:
    """Convert bytes to human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0

def process_blob(blob: storage.Blob, result_queue: Queue) -> None:
    """Process a single blob and add it to the result queue."""
    try:
        logging.info(f"Processing file: {blob.name}, size: {format_size(blob.size)}")
        with blob.open("rb") as f:
            content = f.read()
        result_queue.put((blob.name, content))
    except Exception as exc:
        logging.error(f"Error processing {blob.name}: {exc}")

def get_uploaded_chunks(destination_bucket: storage.Bucket, source_bucket_name: str) -> Set[str]:
    """Get a list of already uploaded zip chunks."""
    prefix = f"{source_bucket_name}/"
    blobs = destination_bucket.list_blobs(prefix=prefix)
    return {blob.name.split('/')[-1] for blob in blobs if blob.name.endswith('.zip')}

def get_last_processed_info(destination_bucket: storage.Bucket, source_bucket_name: str) -> Tuple[Optional[str], int]:
    """Get the name of the last processed file and page number from the manifest."""
    manifest_blob = destination_bucket.blob(f'{source_bucket_name}/manifest.txt')
    if not manifest_blob.exists():
        return None, 0
    
    manifest_content = manifest_blob.download_as_text()
    lines = manifest_content.split('\n')
    if len(lines) < 2:
        return None, 0
    
    last_chunk = lines[-1]
    last_chunk_blob = destination_bucket.blob(f'{source_bucket_name}/{last_chunk}')
    
    with last_chunk_blob.open("rb") as f:
        with zipfile.ZipFile(f, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            if file_list:
                last_file = file_list[-1]
                page_number = int(last_chunk.split('_')[1])
                return last_file, page_number
    
    return None, 0

@profile
def zip_and_upload_page(
    page_blobs: List[storage.Blob],
    destination_bucket: storage.Bucket,
    source_bucket_name: str,
    page_number: int,
    max_workers: int,
    uploaded_chunks: Set[str],
    last_processed_file: Optional[str]
) -> int:
    """Zip and upload a page of blobs."""
    zip_buffer = io.BytesIO()
    result_queue: Queue = Queue()
    processed_files = 0
    zip_chunk_number = 1
    zip_size = 0
    MAX_ZIP_SIZE = 1024 * 1024 * 1024  # 1GB in bytes
    resume_processing = last_processed_file is None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_blob, blob, result_queue) for blob in page_blobs]
        
        for future in as_completed(futures):
            future.result()  # This will raise any exceptions that occurred during processing
            
            while not result_queue.empty():
                file_name, content = result_queue.get()
                
                if not resume_processing:
                    if file_name == last_processed_file:
                        resume_processing = True
                    continue

                # Check if adding this file would exceed the max zip size
                if zip_size + len(content) > MAX_ZIP_SIZE and zip_size > 0:
                    # Upload the current zip file
                    zip_buffer.seek(0)
                    chunk_name = f'page_{page_number:05d}_chunk_{zip_chunk_number:05d}.zip'
                    if chunk_name not in uploaded_chunks:
                        destination_blob = destination_bucket.blob(f'{source_bucket_name}/{chunk_name}')
                        destination_blob.upload_from_file(zip_buffer, content_type='application/zip')
                        logging.info(f"Uploaded zip file for page {page_number}, chunk {zip_chunk_number}")
                        uploaded_chunks.add(chunk_name)
                    else:
                        logging.info(f"Skipped already uploaded zip file for page {page_number}, chunk {zip_chunk_number}")
                    
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
        chunk_name = f'page_{page_number:05d}_chunk_{zip_chunk_number:05d}.zip'
        if chunk_name not in uploaded_chunks:
            destination_blob = destination_bucket.blob(f'{source_bucket_name}/{chunk_name}')
            destination_blob.upload_from_file(zip_buffer, content_type='application/zip')
            logging.info(f"Uploaded final zip file for page {page_number}, chunk {zip_chunk_number}")
            uploaded_chunks.add(chunk_name)
        else:
            logging.info(f"Skipped already uploaded final zip file for page {page_number}, chunk {zip_chunk_number}")

    logging.info(f"Finished creating zip files for page {page_number}. Total files processed: {processed_files}")
    logging.info(f"Total chunks created for page {page_number}: {zip_chunk_number}")

    # Clean up
    zip_buffer.close()
    gc.collect()

    return zip_chunk_number

def is_page_fully_uploaded(uploaded_chunks: Set[str], page_number: int) -> bool:
    """Check if all chunks for a given page are already uploaded."""
    page_chunks = [chunk for chunk in uploaded_chunks if chunk.startswith(f'page_{page_number:05d}_')]
    return len(page_chunks) > 0 and all(f'page_{page_number:05d}_chunk_{i+1:05d}.zip' in uploaded_chunks for i in range(len(page_chunks)))

@profile
def zip_and_upload_bucket(source_bucket_name: str, destination_bucket_name: str, max_workers: int = 10) -> None:
    """Zip and upload the contents of a source bucket to a destination bucket."""
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
        
        # Get already uploaded chunks
        uploaded_chunks = get_uploaded_chunks(destination_bucket, source_bucket_name)
        logging.info(f"Found {len(uploaded_chunks)} already uploaded chunks")
        
        # Get the last processed file and page number
        last_processed_file, last_page_number = get_last_processed_info(destination_bucket, source_bucket_name)
        if last_processed_file:
            logging.info(f"Resuming from last processed file: {last_processed_file} on page {last_page_number}")
        else:
            logging.info("Starting from the beginning")
        
        page_number = 0
        manifest = []
        for page in source_bucket.list_blobs().pages:
            page_number += 1
            
            if is_page_fully_uploaded(uploaded_chunks, page_number):
                logging.info(f"Skipping page {page_number} as it's already fully uploaded")
                # Add all chunks for this page to the manifest
                page_chunks = [chunk for chunk in uploaded_chunks if chunk.startswith(f'page_{page_number:05d}_')]
                manifest.extend(sorted(page_chunks))
                continue
            
            logging.info(f"Processing page {page_number}")
            chunk_count = zip_and_upload_page(page, destination_bucket, source_bucket_name, page_number, max_workers, uploaded_chunks, last_processed_file if page_number == last_page_number else None)
            
            # Add chunk information to manifest
            for chunk in range(1, chunk_count + 1):
                chunk_name = f"page_{page_number:05d}_chunk_{chunk:05d}.zip"
                if chunk_name in uploaded_chunks:
                    manifest.append(chunk_name)
            
            # Reset last_processed_file after processing the page where we resumed
            last_processed_file = None

        # Create or update the manifest file
        manifest_content = f"Total pages: {page_number}\n"
        manifest_content += "\n".join(manifest)
        destination_blob = destination_bucket.blob(f'{source_bucket_name}/manifest.txt')
        destination_blob.upload_from_string(manifest_content)
        logging.info("Uploaded manifest file")
        
        logging.info("Operation completed successfully")
    except Exception as e:
        logging.exception(f"An error occurred: {str(e)}")
        raise

def main():
    """Main function to parse arguments and start the zip and upload process."""
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

if __name__ == "__main__":
    main()