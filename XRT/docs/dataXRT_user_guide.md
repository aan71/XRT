# dataXRT.py - User Documentation

## Overview

`dataXRT.py` is an Exchange Rate Processing Module that automates the workflow of importing exchange rate data into the SICS system. It handles the complete lifecycle of CSV files containing exchange rate records:

1. **Download** files from an AWS S3 pending folder
2. **Process** each record via a SOAP web service
3. **Upload** results to S3 error/processed folders
4. **Clean up** original files from S3 and local storage

---

## Prerequisites

### Software Requirements

- **Python 3.8+**
- Required Python packages:
  - `boto3` - AWS SDK for S3 operations
  - `pandas` - CSV file handling
  - `requests` - HTTP requests
  - `zeep` - SOAP web service client
  - `beautifulsoup4` - File encoding detection
  - `python-dotenv` - Environment variable management

### Installation

```bash
pip install boto3 pandas requests zeep beautifulsoup4 python-dotenv
```

---

## Configuration

### Environment Variables

All sensitive configuration is loaded from environment variables. Create a `.env` file in the project root directory with the following variables:

#### Required Variables

| Variable | Description |
|----------|-------------|
| `AWS_REGION_NAME` | AWS region (e.g., `eu-west-1`) |
| `AWS_ACCESS_KEY_ID` | AWS access key for S3 operations |
| `AWS_SECRET_ACCESS_KEY` | AWS secret access key |
| `WSDL_URL` | URL of the SICS SOAP web service WSDL |
| `WS_USERNAME` | Web service authentication username |
| `WS_PASSWORD` | Web service authentication password |

#### Optional Variables (with defaults)

| Variable | Default Value | Description |
|----------|---------------|-------------|
| `S3_BUCKET_NAME` | `cesce-sics` | S3 bucket name |
| `S3_PENDING_FOLDER` | `noprod/inbound/exchange_rate/pending/` | S3 folder for input files |
| `S3_ERROR_FOLDER` | `noprod/inbound/exchange_rate/error/` | S3 folder for error output |
| `S3_PROCESSED_FOLDER` | `noprod/inbound/exchange_rate/processed/` | S3 folder for successful output |
| `LOCAL_PENDING_DIR` | `C:/Temp/XRT/Pending/` | Local pending directory |
| `LOCAL_ERROR_DIR` | `C:/Temp/XRT/Error/` | Local error directory |
| `LOCAL_PROCESSED_DIR` | `C:/Temp/XRT/Processed/` | Local processed directory |
| `LOG_DIR` | `C:/Temp/XRT/Log/` | Log file directory |
| `LOG_FILENAME` | `cesce.log` | Log file name |

### Example `.env` File

```ini
# AWS Configuration
AWS_REGION_NAME=eu-west-1
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here

# S3 Configuration
S3_BUCKET_NAME=cesce-sics
S3_PENDING_FOLDER=noprod/inbound/exchange_rate/pending/
S3_ERROR_FOLDER=noprod/inbound/exchange_rate/error/
S3_PROCESSED_FOLDER=noprod/inbound/exchange_rate/processed/

# Local Directories
LOCAL_PENDING_DIR=C:/Temp/XRT/Pending/
LOCAL_ERROR_DIR=C:/Temp/XRT/Error/
LOCAL_PROCESSED_DIR=C:/Temp/XRT/Processed/
LOG_DIR=C:/Temp/XRT/Log/

# Web Service Configuration
WSDL_URL=https://your-sics-server/wsdl
WS_USERNAME=your_username
WS_PASSWORD=your_password
```

---

## Usage

### Running the Script

Execute the script from the command line:

```bash
python dataXRT.py
```

### What Happens During Execution

1. **Initialization**
   - Loads configuration from environment variables
   - Sets up logging to the configured log directory
   - Establishes AWS S3 connection

2. **File Discovery**
   - Lists all CSV files in the S3 pending folder
   - Downloads each file to the local pending directory

3. **Record Processing**
   - Reads each CSV file (auto-detects encoding)
   - Sends each row to the SICS web service
   - Categorizes results as success or error

4. **Output Generation**
   - Creates `*_ok.csv` files for successful records → uploaded to S3 processed folder
   - Creates `*_error.csv` files for failed records (with error messages) → uploaded to S3 error folder

5. **Cleanup**
   - Deletes original files from S3 pending folder
   - Deletes temporary local files

---

## Input File Format

### CSV Requirements

- **Format**: Comma-separated values (CSV)
- **Header**: No header row expected
- **Encoding**: Auto-detected (UTF-8, Latin-1, etc.)

### Example Input File

```csv
USD,EUR,1.0856,2025-12-04
GBP,EUR,1.1723,2025-12-04
JPY,EUR,0.0063,2025-12-04
```

---

## Output Files

### Success File (`*_ok.csv`)

Contains all records that were successfully processed by the web service.

- **Location**: S3 processed folder + local processed directory
- **Naming**: Original filename with `_ok` suffix (e.g., `rates.csv` → `rates_ok.csv`)

### Error File (`*_error.csv`)

Contains records that failed processing, with an additional column containing the error message.

- **Location**: S3 error folder + local error directory
- **Naming**: Original filename with `_error` suffix (e.g., `rates.csv` → `rates_error.csv`)
- **Format**: Original columns + error message as last column

---

## Logging

### Log Location

Logs are written to the configured `LOG_DIR` directory (default: `C:/Temp/XRT/Log/cesce.log`).

### Log Format

```
2025-12-04 14:30:00,123 - XRT_0001234567 - INFO - XRT process started
2025-12-04 14:30:01,456 - XRT_0001234567 - INFO - Found 2 files in S3 folder: noprod/inbound/exchange_rate/pending/
```

### Log Levels

- **INFO**: Normal operation messages
- **ERROR**: Processing failures and exceptions

---

## Error Handling

### Common Errors

| Error Type | Cause | Resolution |
|------------|-------|------------|
| `EnvironmentError` | Missing required environment variable | Check `.env` file for missing values |
| `S3OperationError` | AWS S3 operation failed | Verify AWS credentials and permissions |
| `WebServiceError` | SOAP service call failed | Check WSDL URL and credentials |
| `FileProcessingError` | Local file operation failed | Verify directory permissions |

### Web Service Errors

When the SICS web service rejects a record, the error message is captured and appended to the error output file. Common web service errors include:

- Invalid currency codes
- Invalid date formats
- Duplicate records
- Connection timeouts

---

## Directory Structure

After execution, the following directory structure is maintained:

```
C:/Temp/XRT/
├── Pending/          # Temporary storage during processing (cleared after)
├── Processed/        # Successfully processed files (*_ok.csv)
├── Error/            # Files with errors (*_error.csv)
└── Log/              # Application logs (cesce.log)
```

---

## Troubleshooting

### Script Doesn't Start

1. Verify Python version: `python --version` (requires 3.8+)
2. Check all required packages are installed
3. Verify `.env` file exists and contains all required variables

### No Files Processed

1. Check S3 pending folder path is correct
2. Verify AWS credentials have `s3:ListObjects` and `s3:GetObject` permissions
3. Confirm files exist in the S3 pending folder

### All Records Fail

1. Verify WSDL URL is accessible
2. Check web service credentials
3. Review error messages in the `*_error.csv` output files
4. Check log file for detailed error information

### Permission Errors

1. Ensure local directories are writable
2. Verify AWS credentials have necessary S3 permissions:
   - `s3:ListBucket`
   - `s3:GetObject`
   - `s3:PutObject`
   - `s3:DeleteObject`

---

## Security Considerations

- **Never commit** the `.env` file to version control
- Store AWS credentials securely
- Use IAM roles with minimal required permissions
- Rotate credentials regularly
- Web service credentials should be stored securely

---

## Support

- **Author**: Alberto Angelini
- **Date**: December 04, 2025
- **Log Files**: Check `C:/Temp/XRT/Log/cesce.log` for detailed execution logs
