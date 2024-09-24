# Bucket Zip

Bucket Zip is a Python-based tool designed to zip the entire contents of a Google Cloud Storage (GCS) bucket and upload the resulting zip file (or split parts if it's too large) to another GCS bucket.

## Features

- Zip all contents of a source GCS bucket into a single zip file
- Upload the zipped file to a destination GCS bucket
- Automatic splitting of large zip files into smaller parts
- Configurable maximum zip file size before splitting
- Concurrent processing of blobs for improved performance
- Configurable through command-line arguments
- Logging capabilities

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

- `--max-zip-size <int>`: Maximum size of the zip file before splitting (in MB, default: 1024 MB, which is 1GB)
- `--max-workers <int>`: Maximum number of concurrent workers (default: 10)

### Examples:

1. Basic usage:
```bash
python bucket_zip.py my-source-bucket my-destination-bucket
```

2. Set maximum zip size to 500MB:
```bash
python bucket_zip.py my-source-bucket my-destination-bucket --max-zip-size 500
```

3. Customize concurrency:
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

3. Run the script with custom maximum zip size (e.g., 500MB):
```bash
make run SOURCE_BUCKET=my-source-bucket DEST_BUCKET=my-destination-bucket MAX_ZIP_SIZE=500
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

## Note on Recent Changes

The script now zips the entire bucket contents into a single zip file before potentially splitting it into parts if it exceeds the maximum size. The `--max-zip-size` option allows you to set the maximum size of the zip file in MB before it gets split into parts. This change provides more flexibility in handling large buckets and ensures that the contents are zipped together before any splitting occurs.