# dataCED.py User Documentation

## Overview

`dataCED.py` is a Python module that processes CSV files from an AWS S3 pending directory, inserts records into the `CEDE_INTERFACE` database table, and manages the file lifecycle by moving files between pending, processed, and error folders.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Directory Structure](#directory-structure)
5. [Usage](#usage)
6. [Workflow](#workflow)
7. [CSV File Format](#csv-file-format)
8. [Output Files](#output-files)
9. [Logging](#logging)
10. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Software
- Python 3.9 or higher
- ODBC Driver for SQL Server

### Required Python Packages
- `boto3` - AWS SDK for Python
- `botocore` - AWS low-level client
- `beautifulsoup4` - For encoding detection (UnicodeDammit)
- `numpy` - Numerical operations
- `pandas` - Data manipulation
- `pyodbc` - Database connectivity
- `python-dotenv` - Environment variable management

Install dependencies:
```bash
pip install boto3 botocore beautifulsoup4 numpy pandas pyodbc python-dotenv
```

---

## Configuration

### Environment Variables

The module requires the following environment variables. Create a `.env` file in the project root or set them in your system environment:

#### Database Configuration
| Variable | Description | Example |
|----------|-------------|---------|
| `CED_DB_SERVER` | SQL Server hostname or IP | `myserver.database.windows.net` |
| `CED_DB_DATABASE` | Database name | `CEDE_DB` |
| `CED_DB_USERNAME` | Database username | `db_user` |
| `CED_DB_PASSWORD` | Database password | `SecurePassword123` |
| `CED_DB_DRIVER` | ODBC driver name | `ODBC Driver 17 for SQL Server` |

#### AWS S3 Configuration
| Variable | Description | Example |
|----------|-------------|---------|
| `AWS_REGION` | AWS region | `eu-west-1` |
| `AWS_ACCESS_KEY_ID` | AWS access key | `AKIAIOSFODNN7EXAMPLE` |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` |
| `AWS_S3_BUCKET` | S3 bucket name | `my-cede-bucket` |

### Example `.env` File
```env
# Database Configuration
CED_DB_SERVER=myserver.database.windows.net
CED_DB_DATABASE=CEDE_DB
CED_DB_USERNAME=db_user
CED_DB_PASSWORD=SecurePassword123
CED_DB_DRIVER=ODBC Driver 17 for SQL Server

# AWS Configuration
AWS_REGION=eu-west-1
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_S3_BUCKET=my-cede-bucket
```

---

## Directory Structure

### Local Directories
| Directory | Path | Purpose |
|-----------|------|---------|
| Pending | `C:/Temp/CED/Pending/` | Temporary storage for downloaded S3 files |
| Processed | `C:/Temp/CED/Processed/` | Successfully processed records (`*_ok.csv`) |
| Error | `C:/Temp/CED/Error/` | Failed records with error messages (`*_error.csv`) |
| Logs | `C:/Temp/` | Log file location (`ced.log`) |

### S3 Folder Structure
| Folder | Path | Purpose |
|--------|------|---------|
| Pending | `prod/inbound/cede_interface/pending/` | Input files to be processed |
| Processed | `prod/inbound/cede_interface/processed/` | Successfully processed files |
| Error | `prod/inbound/cede_interface/error/` | Files with processing errors |

---

## Usage

### Running the Script

#### Direct Execution
```bash
python dataCED.py
```

#### Using the Batch File
```bash
run_dataCED.bat
```

### Execution Flow
1. The script automatically downloads files from S3 pending folder
2. Processes each CSV file and inserts records into the database
3. Uploads result files to appropriate S3 folders
4. Cleans up local and S3 pending files

---

## Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│                        START                                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 1: Download files from S3 pending folder                  │
│  s3://bucket/prod/inbound/cede_interface/pending/               │
│  → C:/Temp/CED/Pending/                                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 2: Process each CSV file                                  │
│  - Detect file encoding                                         │
│  - Validate required columns                                    │
│  - Insert records into CEDE_INTERFACE table                     │
│  - Track successful and failed records                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 3: Generate output files                                  │
│  - *_ok.csv → C:/Temp/CED/Processed/                           │
│  - *_error.csv → C:/Temp/CED/Error/                            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 4: Upload results to S3                                   │
│  - *_ok.csv → s3://bucket/.../processed/                       │
│  - *_error.csv → s3://bucket/.../error/                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 5: Cleanup                                                │
│  - Delete original file from S3 pending folder                  │
│  - Delete local pending file                                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         END                                      │
└─────────────────────────────────────────────────────────────────┘
```

---

## CSV File Format

### Requirements
- **Delimiter**: Semicolon (`;`)
- **Encoding**: Auto-detected (UTF-8, Latin-1, etc.)
- **Header Row**: Required with exact column names

### Required Columns (168 total)

The CSV file must contain all columns listed below. The order in the CSV does not matter, but all columns must be present.

<details>
<summary>Click to expand full column list</summary>

| Column Name | Description |
|-------------|-------------|
| `PID` | Process ID |
| `STATUS` | Record status |
| `IMPORT_TS` | Import timestamp |
| `BATCH_NAME` | Batch identifier |
| `CREATION_TS` | Creation timestamp |
| `RECORD_TYPE` | Type of record |
| `RECORD_UNIQUE_ID` | Unique record identifier |
| `PRIM_SYS` | Primary system |
| `ACCESS_CODE` | Access code |
| `BASE_COMPANY` | Base company |
| `EC` | Entity code |
| `CURRENCY` | Currency code |
| `DTL_AMT` | Detail amount |
| `AS_AT` | As-at date |
| `OCC_YR` | Occurrence year |
| `UW_YEAR` | Underwriting year |
| `AC_YEAR` | Accounting year |
| `AC_REF_PERIOD` | Accounting reference period |
| `AC_START_DATE` | Accounting start date |
| `AC_END_DATE` | Accounting end date |
| `DATE_OF_BOOKING` | Booking date |
| `BOOKING_YEAR` | Booking year |
| `BOOKING_YEAR_2` | Secondary booking year |
| `BOOKING_YEAR_3` | Tertiary booking year |
| `BOOKING_PERIOD` | Booking period |
| `BOOKING_PERIOD2` | Secondary booking period |
| `BOOKING_PERIOD3` | Tertiary booking period |
| `DTL_COMMENT` | Detail comment |
| `CEDE` | Cede indicator |
| `WS_IDENTIFIER` | Worksheet identifier |
| `WS_TITLE` | Worksheet title |
| `IS_ESTIMATE` | Estimate flag |
| `UDF_TXT1` - `UDF_TXT4` | User-defined text fields |
| `POLICY_ID` | Policy identifier |
| `POLICY_TITLE` | Policy title |
| `POLICY_FORMER_ID` | Former policy ID |
| `IP_START` | Inception period start |
| `IP_END` | Inception period end |
| `ORIGINAL_IP_START` | Original inception start |
| `ORIGINAL_IP_END` | Original inception end |
| `AUTOMATIC_PROT_ASS` | Automatic protection assignment |
| `REASON_FOR_MANPROT` | Reason for manual protection |
| `SECTION_EXT_ID` | Section external ID |
| `SECTION_NAME` | Section name |
| `ATT_FROM` | Attachment from |
| `ATT_TO` | Attachment to |
| `SEC_CURRENCY` | Section currency |
| `MAIN_LIMIT` | Main limit |
| `MAIN_LIMIT_TYPE` | Main limit type |
| `TOTAL_GROSS_P` | Total gross premium |
| `SHARE_PCT` | Share percentage |
| `COUNTRY` | Country code |
| `COUNTRY_GRP` | Country group |
| `STATE` | State code |
| `STATE_GRP` | State group |
| `MCOB` | Major class of business |
| `COB` | Class of business |
| `SCOB` | Sub-class of business |
| `ADDL_CLASS_1` - `ADDL_CLASS_11` | Additional classification fields |
| `REP_UNIT_1` - `REP_UNIT_3` | Reporting units |
| `ORIGIN_OF_BUS` | Origin of business |
| `PERIL` | Peril code |
| `CLAIM_ID` | Claim identifier |
| `HL_LOSS_ID` | High-level loss ID |
| `HL_LOSS_NAME` | High-level loss name |
| `DOL_BEGIN` | Date of loss begin |
| `DOL_END` | Date of loss end |
| `INCL_IN_REC_ORDER` | Include in reconciliation order |
| `ACC_AS_OF_DATE` | Accumulation as-of date |
| `CAUSE_OF_LOSS` | Cause of loss |
| `CONSEQUENCE_OF_LOSS` | Consequence of loss |
| `CLAIM_NAME` | Claim name |
| `RISKNAME` | Risk name |
| `CLAIM_UDF_TXT1` - `CLAIM_UDF_TXT3` | Claim user-defined text |
| `PC_*` | Property/Casualty specific fields |
| `LF_*` | Life-specific fields |
| `UDF_PCT1` - `UDF_PCT4` | User-defined percentage fields |
| `UDF_AMT1_AMT` - `UDF_AMT5_AMT` | User-defined amount fields |
| `FK_UDF_AMT1_CY` - `FK_UDF_AMT5_CY` | Amount currency fields |
| `VERSION` | Record version |

</details>

### Sample CSV Structure
```csv
PID;STATUS;IMPORT_TS;BATCH_NAME;CREATION_TS;RECORD_TYPE;...
001;NEW;2024-01-15 10:30:00;BATCH_001;2024-01-15 10:00:00;PREMIUM;...
002;NEW;2024-01-15 10:30:00;BATCH_001;2024-01-15 10:00:00;CLAIM;...
```

---

## Output Files

### Success File (`*_ok.csv`)
- **Location**: `C:/Temp/CED/Processed/` and S3 processed folder
- **Naming**: Original filename with `_ok` suffix (e.g., `data.csv` → `data_ok.csv`)
- **Content**: All records that were successfully inserted into the database

### Error File (`*_error.csv`)
- **Location**: `C:/Temp/CED/Error/` and S3 error folder
- **Naming**: Original filename with `_error` suffix (e.g., `data.csv` → `data_error.csv`)
- **Content**: Failed records with an additional `ERROR` column containing the error message

---

## Logging

### Log File
- **Location**: `C:/Temp/ced.log`
- **Format**: `%(asctime)s - %(name)s - %(levelname)s - %(message)s`

### Log Levels
| Level | Configuration | Description |
|-------|---------------|-------------|
| INFO | `log_level: "info"` | Standard operational messages |
| DEBUG | `log_level: "debug"` | Detailed debugging (includes SQL statements) |

### Sample Log Output
```
2024-01-15 10:30:00,123 - CED_abc123 - INFO - CED process has been created.
2024-01-15 10:30:01,456 - CED_abc123 - INFO - Step 1: Downloading files from S3
2024-01-15 10:30:02,789 - CED_abc123 - INFO - Downloaded: prod/inbound/.../data.csv -> C:/Temp/CED/Pending/data.csv
2024-01-15 10:30:03,012 - CED_abc123 - INFO - Processing file: data.csv
2024-01-15 10:30:05,345 - CED_abc123 - INFO - Processed successfully: 100 records
```

---

## Troubleshooting

### Common Errors

#### Missing Environment Variables
```
EnvironmentError: Missing required environment variables: CED_DB_SERVER, CED_DB_PASSWORD
```
**Solution**: Ensure all required environment variables are set in your `.env` file or system environment.

#### Database Connection Failed
```
Database error processing file: [ODBC Driver 17 for SQL Server]Login failed
```
**Solution**: 
- Verify database credentials
- Check network connectivity to the database server
- Ensure the ODBC driver is installed

#### CSV Validation Error
```
Validation error in file data.csv: CSV file is missing required columns: ['COLUMN_NAME']
```
**Solution**: Ensure your CSV file contains all required columns with exact column names.

#### S3 Access Denied
```
S3 error with file data.csv: An error occurred (AccessDenied)
```
**Solution**:
- Verify AWS credentials
- Check IAM permissions for the S3 bucket
- Ensure the bucket name is correct

#### File Encoding Issues
```
UnicodeDecodeError: 'utf-8' codec can't decode byte
```
**Solution**: The script auto-detects encoding, but if issues persist, ensure your CSV is saved with UTF-8 encoding.

### Best Practices

1. **Test with small files first** - Validate your CSV format with a few records before processing large batches
2. **Monitor log files** - Check `ced.log` for detailed error information
3. **Review error files** - The `*_error.csv` files contain specific error messages for each failed record
4. **Backup important data** - Always maintain backups before processing large data imports
5. **Use debug mode sparingly** - Debug logging may expose sensitive data; use only in development

---

## Security Notes

- Database credentials are loaded from environment variables (never hardcoded)
- TLS encryption is enabled for database connections
- Error messages are sanitized to remove sensitive information (file paths, passwords)
- Path traversal protection is implemented for file operations
- Cryptographically secure random identifiers are used for logging

---

## Support

For issues or questions, contact the CED Team.

**Author**: CED Team  
**Version**: 1.0  
**Last Updated**: December 2024
