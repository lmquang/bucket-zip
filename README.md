# Bucket Zip

Bucket Zip is a Python-based tool designed to zip the contents of a Google Cloud Storage (GCS) bucket and upload the resulting zip files to another GCS bucket. It supports creating multiple 1GB zip chunks for efficient handling of large data volumes and provides robust resumption capabilities.

## Features

- Zip contents of a source GCS bucket into multiple 1GB zip chunks
- Upload the zipped chunks to a destination GCS bucket
- Automatic creation of new zip chunks when the 1GB limit is reached
- Concurrent processing of blobs for improved performance
- Robust resumption capabilities for interrupted operations
- Efficient skipping of already processed pages and files
- Configurable through command-line arguments
- Detailed logging capabilities
- Creates a manifest file for easy reconstruction of the original data structure

## Requirements

To run this project, you need:

- Python 3.7 or higher
- Dependencies listed in `requirements.txt`

## Installation and Setup

1. Clone this repository:
   ```bash
   git clone https://github.com/your-username/bucket-zip.git
   cd bucket-zip
   ```

2. Create and activate a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   ```

3. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up your environment variables in the `.env` file:
   ```bash
   cp .env.example .env
   # Edit .env file with your actual GCP_SA_KEY
   ```

5. Ensure your IDE is using the correct Python interpreter:
   - In VSCode, press `Ctrl+Shift+P` (or `Cmd+Shift+P` on macOS)
   - Type "Python: Select Interpreter" and choose the interpreter from your virtual environment

## Troubleshooting

If you encounter issues with imports not being resolved (e.g., `dotenv` import):

1. Make sure you've activated the virtual environment
2. Verify that all dependencies are installed:
   ```bash
   pip list
   ```
3. If using an IDE like VSCode, ensure it's using the correct Python interpreter from your virtual environment
4. Try restarting your IDE or terminal

## Usage

To use Bucket Zip, run the main script with the following syntax:

```bash
python bucket_zip.py <source_bucket> <destination_bucket> [options]
```

### Arguments:

- `source_bucket`: Name of the source GCS bucket
- `destination_bucket`: Name of the destination GCS bucket

### Options:

- `--max-workers <int>`: Maximum number of concurrent workers (default: 10)

### Examples:

1. Basic usage:
```bash
python bucket_zip.py my-source-bucket my-destination-bucket
```

2. Customize concurrency:
```bash
python bucket_zip.py my-source-bucket my-destination-bucket --max-workers 20
```

## Configuration

The project uses environment variables for configuration. Set these in the `.env` file before running the script:

- `GCP_SA_KEY`: Base64 encoded Google Cloud service account key JSON

## Build and Automation

This project includes a Makefile for build automation and task running. To see available commands, run:

```bash
make help
```

### Common Makefile Commands:

1. Install dependencies:
```bash
make install
```

2. Run the script with default settings:
```bash
make run SOURCE_BUCKET=my-source-bucket DEST_BUCKET=my-destination-bucket
```

3. Run the script with custom number of workers:
```bash
make run SOURCE_BUCKET=my-source-bucket DEST_BUCKET=my-destination-bucket MAX_WORKERS=20
```

4. Clean up the project (remove virtual environment and compiled Python files):
```bash
make clean
```

## Logging

The script generates logs in `bucket_zip.log`. Refer to this file for detailed operation information and troubleshooting. Console output is also available for basic progress tracking.

## Contributing

Contributions to Bucket Zip are welcome. Please ensure to update tests as appropriate.

## License

[License information to be added]

## Recent Improvements

1. Efficient Page-Level Processing:
   - The script now checks if an entire page of files has been uploaded before processing.
   - Fully uploaded pages are skipped, improving efficiency during resumptions.

2. Robust Resumption Capabilities:
   - Can resume from the last processed file within a partially processed page.
   - Automatically detects and skips already uploaded chunks.

3. Manifest File Enhancements:
   - The manifest file now accurately reflects all uploaded chunks, including those from previously completed runs.

4. Performance Optimization:
   - Reduced redundant processing by implementing smart checks for completed work.

These improvements significantly enhance the script's ability to handle large data volumes efficiently, especially in scenarios where the process might be interrupted and resumed multiple times.