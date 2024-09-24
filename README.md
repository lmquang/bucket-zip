# Bucket Zip

Bucket Zip is a Python-based tool designed to zip the contents of a Google Cloud Storage (GCS) bucket and upload the resulting zip files to another GCS bucket. It now supports creating multiple 1GB zip chunks for efficient handling of large data volumes.

## Features

- Zip contents of a source GCS bucket into multiple 1GB zip chunks
- Upload the zipped chunks to a destination GCS bucket
- Automatic creation of new zip chunks when the 1GB limit is reached
- Concurrent processing of blobs for improved performance
- Configurable through command-line arguments
- Logging capabilities
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

## Note on Recent Changes

The script now creates multiple 1GB zip chunks when processing the bucket contents. This change allows for more efficient handling of large data volumes and ensures that each zip file is of a manageable size. A manifest file is created to keep track of all the chunks, enabling easy reconstruction of the original data structure. The `--max-zip-size` option has been removed as the chunk size is now fixed at 1GB.