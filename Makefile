.PHONY: all venv install run clean help

VENV := venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

# Default bucket names (replace these with your actual bucket names)
SOURCE_BUCKET ?= example-source-bucket
DEST_BUCKET ?= example-destination-bucket

# GCP Service Account Key (base64 encoded JSON)
# This can be set in .env file or as an environment variable
# For .env file, create a file named .env in the same directory as this Makefile with the following content:
# GCP_SA_KEY=your_base64_encoded_service_account_json
GCP_SA_KEY ?= $(shell echo "$$GCP_SA_KEY")

all: venv install run

venv:
	python3 -m venv $(VENV)

# Install dependencies from requirements.txt
install: venv
	$(PIP) install -r requirements.txt

# Usage: make run SOURCE_BUCKET=your-source-bucket DEST_BUCKET=your-destination-bucket
run: install
	@if [ ! -f .env ] && [ -z "$(GCP_SA_KEY)" ]; then \
		echo "Error: Neither .env file nor GCP_SA_KEY environment variable is set."; \
		echo "Please either create a .env file with GCP_SA_KEY or set it as an environment variable."; \
		echo "Example for .env file:"; \
		echo "GCP_SA_KEY=your_base64_encoded_service_account_json"; \
		echo "Example for environment variable:"; \
		echo "export GCP_SA_KEY=\$$(base64 -w 0 path/to/your/service-account-key.json)"; \
		exit 1; \
	fi
	$(PYTHON) bucket_zip.py $(SOURCE_BUCKET) $(DEST_BUCKET)

clean:
	rm -rf $(VENV)
	rm -f *.pyc
	rm -rf __pycache__

help:
	@echo "Usage:"
	@echo "  make install    - Create virtual environment and install dependencies from requirements.txt"
	@echo "  make run        - Run the script with default bucket names"
	@echo "  make run SOURCE_BUCKET=your-source-bucket DEST_BUCKET=your-destination-bucket"
	@echo "                  - Run the script with custom bucket names"
	@echo "  make clean      - Remove virtual environment and compiled Python files"
	@echo "  make help       - Show this help message"
	@echo ""
	@echo "Before running, make sure to set the GCP_SA_KEY:"
	@echo "Option 1: Create a .env file in the same directory with the following content:"
	@echo "GCP_SA_KEY=your_base64_encoded_service_account_json"
	@echo ""
	@echo "Option 2: Set it as an environment variable:"
	@echo "export GCP_SA_KEY=\$$(base64 -w 0 path/to/your/service-account-key.json)"
	@echo ""
	@echo "Dependencies are listed in requirements.txt and will be installed automatically with 'make install'"

# Set help as the default target
.DEFAULT_GOAL := help